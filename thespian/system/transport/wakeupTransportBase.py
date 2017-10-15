"""This module provides a mixin base class for transports that
implements support for wakeupAfter() timed messages."""


from thespian.actors import *
from thespian.system.timing import ExpirationTimer, currentTime
from thespian.system.transport import *
import threading


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
        # _pendingWakeups is a sorted list of tuples, containing an
        # ExpirationTimer and a payload. It is sorted by ExpirationTimer
        # from the shortest to the longest.
        self._activeWakeups = []  # expired wakeups to be delivered
        self._wakeup_lock = threading.Lock()  # protects the following:
        self._pendingWakeups = []


    def _updateStatusResponse(self, resp):
        """Called to update a Thespian_SystemStatus or Thespian_ActorStatus
           with common information
        """
        # we only pass the ExpirationTimer without payload as this should be
        # sufficient for status information.
        with self._wakeup_lock:
            resp.addWakeups([(self.myAddress, T[0]) for T in self._pendingWakeups])
            for each in self._activeWakeups:
                resp.addPendingMessage(self.myAddress, self.myAddress, str(each.message))

    def _update_runtime(self):
        self.run_time = self._pendingWakeups[0][0] if self._pendingWakeups else self._max_runtime

    def run(self, incomingHandler, maximumDuration=None):
        """Core scheduling method; called by the current Actor process when
           idle to await new messages (or to do background
           processing).
        """
        self._max_runtime = ExpirationTimer(maximumDuration)

        # Always make at least one pass through to handle expired wakeups
        # and queued events; otherwise a null/negative maximumDuration could
        # block all processing.

        rval = self._run_subtransport(incomingHandler)

        while rval in (True, None) and not self._max_runtime.view().expired():
            rval = self._run_subtransport(incomingHandler)

        return rval

    def _run_subtransport(self, incomingHandler):
        self._update_runtime()  # ok not to lock: read once, no modifications
        rval = self._runWithExpiry(incomingHandler)
        if rval is not None and not isinstance(rval, Thespian__Run_Expired):
            return rval

        self._realizeWakeups()
        return self._deliver_wakeups(incomingHandler)

    def _deliver_wakeups(self, incomingHandler):
        while self._activeWakeups:
            w = self._activeWakeups.pop()
            if incomingHandler in (None, TransmitOnly):
                return w
            r = Thespian__Run_HandlerResult(incomingHandler(w))
            if not r:
                return r
        return None


    def addWakeup(self, timePeriod, payload):
        with self._wakeup_lock:
            self._pendingWakeups.append((ExpirationTimer(timePeriod), payload))
            self._pendingWakeups.sort(key=lambda t: t[0])
        # The addWakeup method is called as a result of
        # self.wakeupAfter, so ensure that the current run time is
        # updated in case this new wakeup is the shortest.
        self._update_runtime()


    def _realizeWakeups(self):
        "Find any expired wakeups and queue them to the send processing queue"
        with self._wakeup_lock:
            ct = currentTime()
            starting_len = len(self._activeWakeups)
            while self._pendingWakeups and self._pendingWakeups[0][0].view(ct).expired():
                timer, payload = self._pendingWakeups.pop(0)
                self._activeWakeups.append(
                    ReceiveEnvelope(self.myAddress,
                                    WakeupMessage(timer.duration, payload)))
        return starting_len != len(self._activeWakeups)
