"""This module provides a mixin base class for transports that
implements support for wakeupAfter() timed messages."""


from thespian.actors import *
from thespian.system.utilis import ExpiryTime
from datetime import datetime
from thespian.system.transport import *


class wakeupTransportBase(object):

    """The wakeupTransportBase is designed to be used as a mixin-base for
       a Transport class and provides handling for the wakeupAfter()
       functionality.

       This base mixin provides the primary .run() entrypoint for the
       transport and a .run_time ExpiryTime member that provides the
       remaining time-to-run period.

       The system can handle .wakeupAfter() requests by calling this
       class's .addWakeup() method with the datetime.timedelta for the
       wakeup to be scheduled.

       The Transport should provide the following:

         ._runWithExpiry(incomingHandler)

              Called by this class's .run() entrypoint to do the
              actual transport-specific run routine.  Should perform
              that activity while the self.run_time ExpiryTime is not
              expired (self.run_time will be updated when new
              wakeupAfter() events are scheduled).
    """

    def __init__(self, *args, **kw):
        super(wakeupTransportBase, self).__init__(*args, **kw)
        # _pendingWakeups: key = datetime for wakeup, value = list of
        # pending wakeupAfter msgs to restart at that time
        self._pendingWakeups = {}
        self._activeWakeups = []  # expired wakeups to be delivered


    def _updateStatusResponse(self, resp):
        "Called to update a Thespian_SystemStatus or Thespian_ActorStatus with common information"
        resp.addWakeups(self._pendingWakeups)
        for each in self._activeWakeups:
            resp.addPendingMessage(self.myAddress, self.myAddress, str(each.message))


    def run(self, incomingHandler, maximumDuration=None):

        """Core scheduling method; called by the current Actor process when
           idle to await new messages (or to do background
           processing).
        """
        self._max_runtime = ExpiryTime(maximumDuration)

        while not self._max_runtime.expired():
            now = datetime.now()
            self.run_time = min([ExpiryTime(P - now) for P in self._pendingWakeups] +
                                [self._max_runtime])
            rval = self._runWithExpiry(incomingHandler)
            if rval is not None:
                return rval

            if not self._realizeWakeups():
                # No wakeups were processed, and the inner run
                # returned, so assume there's nothing to do and exit
                return rval

            while self._activeWakeups:
                w = self._activeWakeups.pop()
                if incomingHandler is None:
                    return w
                if not incomingHandler(w):
                    return None

        return None


    def addWakeup(self, timePeriod):
        now = datetime.now()
        wakeupTime = now + timePeriod
        self._pendingWakeups.setdefault(wakeupTime, []) \
                            .append(ReceiveEnvelope(self.myAddress, WakeupMessage(timePeriod)))
        self.run_time = min([ExpiryTime(P - now) for P in self._pendingWakeups] +
                            [self._max_runtime])


    def _realizeWakeups(self):
        "Find any expired wakeups and queue them to the send processing queue"
        now = datetime.now()
        removals = []
        for wakeupTime in self._pendingWakeups:
            if wakeupTime > now:
                continue
            self._activeWakeups.extend(self._pendingWakeups[wakeupTime])
            removals.append(wakeupTime)
        for each in removals:
            del self._pendingWakeups[each]
        return bool(removals)

    # KWQ: Thespian_ActorStatus pendingWakeups = [W for K,V in self._pendingWakeups.items() for A,W in V]
