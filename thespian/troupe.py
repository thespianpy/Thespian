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

   idle_count -- the default number of actors in the troupe when idle.
                 As work is received, the number of actors will grow
                 up to the max_count, but when there is no more work,
                 the number of actors will shrink back down to this
                 number.  Note that there may be fewer than this
                 number of actors present: actors are only created if
                 work is received and there are no idle actors to
                 handle that work.

"""

from thespian.actors import *

class _TroupeMemberReady(object): pass
class _TroupeWork(object):
    def __init__(self, message, orig_sender):
        self.message = message
        self.orig_sender = orig_sender

class _TroupeManager(object):
    def __init__(self, actorClass, idle_count, max_count):
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
        work = _TroupeWork(msg, sender)
        if self._idle_troupers:
            return [(self._idle_troupers.pop(), work)]
        if len(self._troupers) < self.max_count:
            return [(None, work)]
        self._pending_work.append(work)
        return []
    def add_trouper(self, trouper_addr):
        if trouper_addr not in self._troupers:
            self._troupers.append(trouper_addr)

def troupe(max_count=10, idle_count=2):
    def _troupe(actor):
        def manageTroupe(self, message, sender):
            if isinstance(message, ActorSystemMessage):
                self._orig_receiveMessage(message, sender)
                return
            if isinstance(message, _TroupeWork):
                self._orig_receiveMessage(message.message,
                                          message.orig_sender)
                self.send(sender, _TroupeMemberReady())
                return
            if not hasattr(self, '_troupe_mgr'):
                self._troupe_mgr = _TroupeManager(
                    self.__class__, idle_count, max_count)
            if isinstance(message, _TroupeMemberReady):
                for sendargs in self._troupe_mgr.is_ready(sender):
                    self.send(*sendargs)
            else:
                for sendargs in self._troupe_mgr.new_work(message, sender):
                    if sendargs[0] is None:
                        sendargs = (
                            self.createActor(self.__class__),
                            ) + sendargs[1:]
                        self._troupe_mgr.add_trouper(sendargs[0])
                    self.send(*sendargs)
        actor._orig_receiveMessage = actor.receiveMessage
        actor.receiveMessage = manageTroupe
        return actor
    return _troupe
