"""Decorators for defining Transient actors: actors which go away (on
their own) after performing work.

There are two decorators defined here: Transient and TransientIdle.

  * The Transient decorator marks an Actor that exits a specified
    amount of time after it receives its first message.

  * The TransientIdle decorator marks an Actor that exits after it has
    been idle (i.e. it has not received any messages) for the
    specified period of time.

Both decorators take an optional argument that is the amount of time
before they exit (defaulting to 10 seconds), specified in a form that
is compatible with the 'self.wakeupAfter()' call.

Example:

    @transient(timedelta(seconds=20, minutes=3))
    class Worker(Actor):
        def receiveMessage(self, msg, sender):
            ...

    @transient_idle(timedelta(milliseconds=300))
    class FastWorker(Actor):
        def receiveMessage(self, msg, sender):
            ...

"""

from thespian.actors import ActorExitRequest, WakeupMessage, ActorSystemMessage
from datetime import datetime, timedelta


def transient(exit_delay=timedelta(seconds=10)):
    """Decorator for an Actor that specifies the Actor should exit within
       a specified time period (defaulting to 10 seconds) after it
       receives its first message.
    """
    def _TransientActor(actor_class):
        def receiveMessage(self, msg, sender):
            if not getattr(self, '_TransientExitScheduled', None):
                self.wakeupAfter(exit_delay)
                self._TransientExitScheduled = datetime.now() + exit_delay
            if isinstance(msg, WakeupMessage) and \
               datetime.now() >= self._TransientExitScheduled:
                self.send(self.myAddress, ActorExitRequest())
            return self._TA_rcvmsg(msg, sender)
        actor_class._TA_rcvmsg = actor_class.receiveMessage
        actor_class.receiveMessage = receiveMessage
        return actor_class
    return _TransientActor


def transient_idle(exit_delay=timedelta(seconds=10)):
    """Decorator for an Actor that specifies the Actor should exit after
       it has been idle (not received any messages) for a specified
       time period (defaulting to 10 seconds)
    """
    def _TransientIdleActor(actor_class):
        def receiveMessage(self, msg, sender):
            if not getattr(self, '_TransientIdleExitScheduled', None):
                self.wakeupAfter(exit_delay)
                self._TransientIdleExitScheduled = datetime.now() + exit_delay
            if isinstance(msg, WakeupMessage) and \
               datetime.now() >= self._TransientIdleExitScheduled:
                self.send(self.myAddress, ActorExitRequest())
            elif not isinstance(msg, ActorSystemMessage):
                self.wakeupAfter(exit_delay)
                self._TransientIdleExitScheduled = datetime.now() + exit_delay
            return self._TIA_rcvmsg(msg, sender)
        actor_class._TIA_rcvmsg = actor_class.receiveMessage
        actor_class.receiveMessage = receiveMessage
        return actor_class
    return _TransientIdleActor
