"""Exports the "troupe" decorator that can be applied to Actor
definitions.  This decorator turns a regular Actor into a dynamic
Actor Troupe, where multiple Actors are spun up on-demand to handle
messages.

This pattern is especially useful for situations where multiple
requests are received and processing individual requests may take some
time to perform.

Usage:

from thespian.troupe import troupe

@troupe()
class MyActor(Actor):
   ...


The optional arguments to the troupe decorator are:

   max_count -- the maximum number of actors in the troupe (default=10)

   idle_count -- the number of actors in the troupe when idle (default=2).
                 As work is received, the number of actors will grow
                 up to the max_count, but when there is no more work,
                 the number of actors will shrink back down to this
                 number.  Note that there may be fewer than this
                 number of actors present: actors are only created if
                 work is received and there are no idle actors to
                 handle that work.

The decorator usage above works very well for a simple worker actor
that can perform all of the necessary work utilizing only the message
sent to it; the actor can be turned into a troupe member with no
change other than adding the decorator.

However, an actor which must interact with other actors to process the
work requires additional modifications to allow the troupe manager to
know when the actor has finished performing the work.  A troupe member
that has not fully performed the work and is exchanging messages with
other actors to complete the work (or awaiting WakeupMessages) must
set the "troupe_work_in_progress" attribute on self to True.  Once the
work is completed by a subsequent message delivery, it should set this
attribute to False, which will cause the troupe manager to be notified
that the actor is ready for more work.

Failure to set the "troupe_work_in_progress" attribute to True on a
multi-step actor will result in either (a) the actor receiving more
work before it has completed the previous work, or (b) the actor will
be killed by the troupe manager before finishing the work because the
manager believes the actor is finished.

Failure to reset the "troup_work_in_progress" attribute to False will
cause the troupe manager to never send any more work requests to the
troupe actor, even if the latter is idle.  The troupe actor will also
never be killed until the troupe manager itself is killed.

"""

from thespian.actors import (ActorSystemMessage, ActorExitRequest,
                             ChildActorExited)
import inspect


class _TroupeMemberReady(object):
    pass


class _TroupeWork(object):
    def __init__(self, message, orig_sender, troupe_mgr):
        self.message = message
        self.orig_sender = orig_sender
        self.troupe_mgr = troupe_mgr

    def __str__(self):
        return '_TroupeWork(from=%s, msg=%s)' % \
            (self.orig_sender, self.message)


class _TroupeManager(object):
    def __init__(self, actorClass, mgr_addr, idle_count, max_count):
        self.mgr_addr = mgr_addr
        self.idle_count = idle_count
        self.max_count = max_count
        self._troupers = []
        self._idle_troupers = []
        self._pending_work = []

    def is_ready(self, troupe_member):
        if self._pending_work:
            return [(troupe_member, self._pending_work.pop(0))]
        if self.idle_count is not None and \
           len(self._troupers) > self.idle_count:
            self._troupers.remove(troupe_member)
            return [(troupe_member, ActorExitRequest())]
        self._idle_troupers.append(troupe_member)
        return []

    def new_work(self, msg, sender):
        work = _TroupeWork(msg, sender, self.mgr_addr)
        if self._idle_troupers:
            return [(self._idle_troupers.pop(), work)]
        if len(self._troupers) < self.max_count:
            return [(None, work)]
        self._pending_work.append(work)
        return []

    def add_trouper(self, trouper_addr):
        if trouper_addr not in self._troupers:
            self._troupers.append(trouper_addr)

    def worker_exited(self, trouper_addr):
        self._troupers.remove(trouper_addr)
        self._idle_troupers.remove(trouper_addr)
        # n.b. work held by an exited trouper must be recovered by the
        # dead letter handler

    def status(self):
        return 'Idle=%d, Max=%d, Troupers [%d, %d idle]: %s, Pending=%d' % (
            self.idle_count, self.max_count,
            len(self._troupers), len(self._idle_troupers),
            ['%s%s' % (('I:' if A in self._idle_troupers else ''), str(A))
             for A in self._troupers],
            len(self._pending_work)
        )


def troupe(max_count=10, idle_count=2):
    def _troupe(actorClass):
        actorName = '.'.join((inspect.getmodule(actorClass).__name__,
                              actorClass.__name__))

        def manageTroupe(self, message, sender):
            isTroupeWork = isinstance(message, _TroupeWork)
            if isinstance(message, ActorSystemMessage) or \
               isTroupeWork or getattr(self, '_is_a_troupe_worker', False):
                was_in_prog = getattr(self, 'troupe_work_in_progress', False)
                if isTroupeWork:
                    self._is_a_troupe_worker = message.troupe_mgr
                    r = self._orig_receiveMessage(message.message,
                                                  message.orig_sender)
                else:
                    r = self._orig_receiveMessage(message, sender)
                if (isTroupeWork or was_in_prog) and \
                   not getattr(self, 'troupe_work_in_progress', False):
                    self.send(self._is_a_troupe_worker, _TroupeMemberReady())
                return r
            # The following is only run for the primary/manager of the troupe
            if not hasattr(self, '_troupe_mgr'):
                self._troupe_mgr = _TroupeManager(
                    self.__class__, self.myAddress, idle_count, max_count)
            if isinstance(message, ChildActorExited):
                self._troupe_mgr.worker_exited(message.childAddress)
            elif isinstance(message, _TroupeMemberReady):
                for sendargs in self._troupe_mgr.is_ready(sender):
                    self.send(*sendargs)
            elif message == 'troupe:status?':
                self.send(sender, self._troupe_mgr.status())
            else:
                for sendargs in self._troupe_mgr.new_work(message, sender):
                    if sendargs[0] is None:
                        sendargs = (
                            self.createActor(actorName),
                            ) + sendargs[1:]
                        self._troupe_mgr.add_trouper(sendargs[0])
                    self.send(*sendargs)
        actorClass._orig_receiveMessage = actorClass.receiveMessage
        actorClass.receiveMessage = manageTroupe
        return actorClass
    return _troupe
