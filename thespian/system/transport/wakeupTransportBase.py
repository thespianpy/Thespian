"""This module provides a mixin base class for transports that
implements support for wakeupAfter() timed messages."""


from thespian.actors import *
from thespian.system.timing import ExpirationTimer
from thespian.system.transport import *


class wakeupTransportBase(object):

    """The wakeupTransportBase is designed to be used as a mixin-base for
       a Transport class and provides handling for the wakeupAfter()
       functionality.

       This base mixin provides the primary .run() entrypoint for the
       transport and a .run_time ExpirationTime member that provides the
       remaining time-to-run period.

       The system can handle .wakeupAfter() requests by calling this
       class's .addWakeup() method with the datetime.timedelta for the
       wakeup to be scheduled.

       The Transport should provide the following:

         ._runWithExpiry(incomingHandler)

              Called by this class's .run() entrypoint to do the
              actual transport-specific run routine.  Should perform
              that activity while the self.run_time ExpirationTimer is not
              expired (self.run_time will be updated when new
              wakeupAfter() events are scheduled).
    """

    def __init__(self, *args, **kw):
        super(wakeupTransportBase, self).__init__(*args, **kw)
        # _pendingWakeups is a sorted list of ExpirationTimer objects,
        # from the shortest to the longest.
        self._pendingWakeups = []
        self._activeWakeups = []  # expired wakeups to be delivered


    def _updateStatusResponse(self, resp):
        """Called to update a Thespian_SystemStatus or Thespian_ActorStatus
           with common information
        """
        resp.addWakeups([(self.myAddress, T) for T in self._pendingWakeups])
        for each in self._activeWakeups:
            resp.addPendingMessage(self.myAddress, self.myAddress, str(each.message))

    def _update_runtime(self):
        self.run_time = (self._pendingWakeups + [self._max_runtime])[0]

    def run(self, incomingHandler, maximumDuration=None):
        """Core scheduling method; called by the current Actor process when
           idle to await new messages (or to do background
           processing).
        """
        self._max_runtime = ExpirationTimer(maximumDuration)

        # Always make at least one pass through to handle expired wakeups
        # and queued events; otherwise a null/negative maximumDuration could
        # block all processing.
        firstTime = True

        while firstTime or not self._max_runtime.expired():
            firstTime = False
            self._update_runtime()
            rval = self._runWithExpiry(incomingHandler)
            if rval is not None:
                return rval

            if not self._realizeWakeups():
                # No wakeups were processed, and the inner run
                # returned, so assume there's nothing to do and exit
                return rval

            while self._activeWakeups:
                w = self._activeWakeups.pop()
                if incomingHandler in (None, TransmitOnly):
                    return w
                if not incomingHandler(w):
                    return None

        return None


    def addWakeup(self, timePeriod):
        self._pendingWakeups.append(ExpirationTimer(timePeriod))
        self._pendingWakeups.sort()
        # The addWakeup method is called as a result of
        # self.wakeupAfter, so ensure that the current run time is
        # updated in case this new wakeup is the shortest.
        self._update_runtime()


    def _realizeWakeups(self):
        "Find any expired wakeups and queue them to the send processing queue"
        starting_len = len(self._activeWakeups)
        while self._pendingWakeups and self._pendingWakeups[0].expired():
            self._activeWakeups.append(
                ReceiveEnvelope(self.myAddress,
                                WakeupMessage(self._pendingWakeups.pop(0).duration)))
        return starting_len != len(self._activeWakeups)
