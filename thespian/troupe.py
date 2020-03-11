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

Failure to reset the "troupe_work_in_progress" attribute to False will
cause the troupe manager to never send any more work requests to the
troupe actor, even if the latter is idle.  The troupe actor will also
never be killed until the troupe manager itself is killed.

"""

from thespian.actors import (ActorSystemMessage, ActorExitRequest,
                             ChildActorExited, WakeupMessage)
from datetime import timedelta
import inspect


# If at least some troupe members have been idle for this long and
# they are over the idle count, they can be dismissed (killed).
DISMISS_EXTRA_PERIOD = timedelta(seconds=2)


class UpdateTroupeSettings(object):
    """A message that can be sent to a Troupe to cause the troupe manager
       to update either or both of the max_count and idle_count number
       of workers.  The Troupe manager will respond by sending a
       message of this type back with the limit values in effect after
       the update.

    """
    def __init__(self, max_count=None, idle_count=None):
        if max_count:
            assert max_count > 0
        if idle_count:
            assert idle_count > 0
        self.max_count = max_count
        self.idle_count = idle_count


class _TroupeMemberReady(object):
    def __init__(self, work_ident):
        self.ident_done = work_ident


class _TroupeWork(object):
    def __init__(self, message, orig_sender, troupe_mgr):
        self.message = message
        self.orig_sender = orig_sender
        self.troupe_mgr = troupe_mgr
        self.ident = None

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
        self._extra_troupers = []
        self._pending_work = []
        self._handling_work = {}
        self._pending_dismissal = False
        self._work_ident = 0

    def is_ready(self, managerActor, ready_msg, troupe_member):
        if ready_msg.ident_done >= 0:
            if ready_msg.ident_done in self._handling_work:
                del self._handling_work[ready_msg.ident_done]
        if self._pending_work:
            w = self._pending_work.pop(0)
            self._handling_work[w.ident] = (troupe_member, w)
            return [(troupe_member, w)]
        if self.idle_count is not None and \
           len(self._troupers) > self.idle_count and \
           len(self._idle_troupers) >= self.idle_count:
            self._trouper_is_extra(managerActor, troupe_member)
            return []
        self._idle_troupers.append(troupe_member)
        return []

    def _trouper_is_extra(self, managerActor, troupe_member):
        if not self._pending_dismissal:
            managerActor.wakeupAfter(DISMISS_EXTRA_PERIOD)
            self._pending_dismissal = True
        self._extra_troupers.append(troupe_member)

    def dismiss_extras(self, managerActor):
        exitReq = ActorExitRequest()
        for each in self._extra_troupers:
            managerActor.send(each, exitReq)
        self._extra_troupers = []
        self._pending_dismissal = False

    def new_work(self, msg, sender):
        if isinstance(msg, _TroupeWork):
            work = msg
        else:
            work = _TroupeWork(msg, sender, self.mgr_addr)
            work.ident = self._work_ident
            # limit of 0xffffffff is > max reasonable pending work items
            self._work_ident = (self._work_ident + 1) & 0xffffffff
        worker = self._idle_troupers.pop(0) if self._idle_troupers else \
                 (self._extra_troupers.pop(0) if self._extra_troupers else None)
        if worker:
            self._handling_work[work.ident] = (worker, work)
            return [(worker, work)]
        if len(self._troupers) < self.max_count:
            return [(None, work)]
        self._pending_work.append(work)
        return []

    def add_trouper(self, trouper_addr, trouper_work):
        if trouper_addr not in self._troupers:
            self._troupers.append(trouper_addr)
        self._handling_work[trouper_work.ident] = (trouper_addr, trouper_work)

    def worker_exited(self, trouper_addr):
        try:
            self._troupers.remove(trouper_addr)
        except ValueError: pass
        try:
            self._idle_troupers.remove(trouper_addr)
        except ValueError: pass
        try:
            self._extra_troupers.remove(trouper_addr)
        except ValueError: pass
        wcheck = filter(lambda e: e[0] == trouper_addr, self._handling_work.values())
        # n.b. list(wcheck): removing from self._handling_work, which
        # will cause a RuntimeError of "dictionary changed size during
        # iteration" if done on the iterator.
        for (_,wfnd) in list(wcheck):  # should be 0 or 1 entry
            del self._handling_work[wfnd.ident]
            if self._idle_troupers:
                # If idle, re-attempt this work immediately; if not
                # idle, place it on the pending queue to be handled by
                # an existing worker to avoid a fork bomb.
                return wfnd
            self._pending_work.append(wfnd)
        return None

    def status(self):
        return 'Idle=%d, Max=%d, Troupers [%d, %d idle, %d extra]: %s, Pending=%d' % (
            self.idle_count, self.max_count,
            len(self._troupers), len(self._idle_troupers), len(self._extra_troupers),
            ['%s%s' % (('I:' if A in self._idle_troupers else
                        'E:' if A in self._extra_troupers else ''), str(A))
             for A in self._troupers],
            len(self._pending_work)
        )


def troupe(max_count=10, idle_count=2):
    def _troupe(actorClass):
        actorName = '.'.join((inspect.getmodule(actorClass).__name__,
                              actorClass.__name__))

        def manageTroupe(self, message, sender):
            isTroupeWork = isinstance(message, _TroupeWork)
            troupeWorker = getattr(self, '_is_a_troupe_worker', False)
            # If a worker, or this message indicates we are a
            # worker... or we haven't been decided yet but this is a
            # system message so we shouldn't create a troupe because
            # of it.
            if troupeWorker or isTroupeWork or \
               (not hasattr(self, '_troupe_mgr') and isinstance(message, ActorSystemMessage)):
                was_in_prog = getattr(self, 'troupe_work_in_progress', False)
                if isTroupeWork:
                    self._is_a_troupe_worker = message.troupe_mgr
                    self._work_ident = message.ident
                    r = self._orig_receiveMessage(message.message,
                                                  message.orig_sender)
                else:
                    r = self._orig_receiveMessage(message, sender)
                if (isTroupeWork or was_in_prog) and \
                   not getattr(self, 'troupe_work_in_progress', False):
                    self.send(self._is_a_troupe_worker,
                              _TroupeMemberReady(self._work_ident))
                    self._work_ident = -1
                return r
            # The following is only run for the primary/manager of the troupe
            if isinstance(message, ActorExitRequest):
                return
            if not hasattr(self, '_troupe_mgr'):
                self._troupe_mgr = _TroupeManager(
                    self.__class__, self.myAddress, idle_count, max_count)
            if isinstance(message, ChildActorExited):
                message = self._troupe_mgr.worker_exited(message.childAddress)
            elif isinstance(message, _TroupeMemberReady):
                for sendargs in self._troupe_mgr.is_ready(self, message, sender):
                    self.send(*sendargs)
                return
            elif isinstance(message, UpdateTroupeSettings):
                if message.max_count:
                    self._troupe_mgr.max_count = message.max_count
                if message.idle_count:
                    self._troupe_mgr.idle_count = message.idle_count
                self.send(sender, UpdateTroupeSettings(max_count=self._troupe_mgr.max_count,
                                                       idle_count=self._troupe_mgr.idle_count))
            elif isinstance(message, str):
                if message == 'troupe:status?':
                    self.send(sender, self._troupe_mgr.status())
                    return
                elif message.startswith('troupe:set_max_count='):
                    try:
                        new_max_count = int(message.split('=')[1])
                    except ValueError as ex:
                        self.send(sender, 'Error changing max_count'
                                  ' for troupe based on message "%s": %s' %
                                  (message, str(ex)))
                        return
                    self._troupe_mgr.max_count = new_max_count
                    self.send(sender,
                              'Set troupe max_count to %d' % new_max_count)
                    return
                elif message.startswith('troupe:set_idle_count='):
                    try:
                        new_idle_count = int(message.split('=')[1])
                    except ValueError as ex:
                        self.send(sender, 'Error changing idle_count'
                                  ' for troupe based on message "%s": %s' %
                                  (message, str(ex)))
                        return
                    self._troupe_mgr.idle_count = new_idle_count
                    self.send(sender,
                              'Set troupe idle_count to %d' % new_idle_count)
                    return
            elif isinstance(message, WakeupMessage):
                self._troupe_mgr.dismiss_extras(self)
                return
            if message:
                for sendargs in self._troupe_mgr.new_work(message, sender):
                    if sendargs[0] is None:
                        sendargs = (self.createActor(actorName), sendargs[1])
                        self._troupe_mgr.add_trouper(*sendargs)
                    self.send(*sendargs)
        actorClass._orig_receiveMessage = actorClass.receiveMessage
        actorClass.receiveMessage = manageTroupe
        return actorClass
    return _troupe
