from thespian.system.utilis import partition
from thespian.system.timing import ExpiryTime
from thespian.system.transport import SendStatus
from datetime import timedelta


HYSTERESIS_MIN_PERIOD  = timedelta(milliseconds=250)
HYSTERESIS_MAX_PERIOD  = timedelta(seconds=45)
HYSTERESIS_RATE        = 1.2


class HysteresisDelaySender(object):
    """Implements hysteresis delay for sending messages.  This is intended
       to be used for messages exchanged between convention members to
       ensure that a mis-behaved member doesn't have the ability to
       inflict damage on the entire convention.  The first time a
       message is sent via this sender it is passed on through, but
       that starts a blackout period that starts with the
       CONVENTION_HYSTERESIS_MIN_PERIOD.  Each additional send attempt
       during that blackout period will cause the blackout period to
       be extended by the CONVENTION_HYSTERESIS_RATE, up to the
       CONVENTION_HYSTERESIS_MAX_PERIOD.  Once the blackout period
       ends, the queued sends will be sent, but only the last
       attempted message of each type for the specified remote target.
       At that point, the hysteresis delay will be reduced by the
       CONVENTION_HYSTERESIS_RATE; further send attempts will affect
       the hysteresis blackout period as described as above but lack
       of sending attempts will continue to reduce the hysteresis back
       to a zero-delay setting.

       Note: delays are updated in a target-independent manner; the
             target is only considered when eliminating duplicates.

       Note: maxDelay on TransmitIntents is ignored by hysteresis
             delays.  It is assumed that a transmit intent's maxDelay
             is greater than the maximum hysteresis period and/or that
             the hysteresis delay is more important than the transmit
             intent timeout.
    """
    def __init__(self, actual_sender,
                 hysteresis_min_period = HYSTERESIS_MIN_PERIOD,
                 hysteresis_max_period = HYSTERESIS_MAX_PERIOD,
                 hysteresis_rate       = HYSTERESIS_RATE):
        self._sender                = actual_sender
        self._hysteresis_until      = ExpiryTime(timedelta(seconds=0))
        self._hysteresis_queue      = []
        self._current_hysteresis    = None  # timedelta
        self._hysteresis_min_period = hysteresis_min_period
        self._hysteresis_max_period = hysteresis_max_period
        self._hysteresis_rate       = hysteresis_rate

    @property
    def delay(self):
        return self._hysteresis_until

    def _has_hysteresis(self):
        return (self._current_hysteresis is not None and
                self._current_hysteresis >= self._hysteresis_min_period)

    def _increase_hysteresis(self):
        if self._has_hysteresis():
            try:
                self._current_hysteresis = min(
                    (self._current_hysteresis * self._hysteresis_rate),
                    self._hysteresis_max_period)
            except TypeError:
                # See note below for _decrease_hysteresis
                self._current_hysteresis = min(
                    timedelta(
                        seconds=(self._current_hysteresis.seconds *
                                 self._hysteresis_rate)),
                    self._hysteresis_max_period)
        else:
            self._current_hysteresis = self._hysteresis_min_period

    def _decrease_hysteresis(self):
        try:
            self._current_hysteresis = (
                (self._current_hysteresis / self._hysteresis_rate)
                if self._has_hysteresis() else None)
        except TypeError:
            # Python 2.x cannot multiply or divide a timedelta by a
            # fractional amount.  There is also not a total_seconds
            # retrieval from a timedelta, but it should be safe to
            # assume that the hysteresis value is not greater than 1
            # day.
            self._current_hysteresis = timedelta(
                seconds=(self._current_hysteresis.seconds /
                         self._hysteresis_rate)) \
                if self._has_hysteresis() else None

    def _update_remaining_hysteresis_period(self, reset=False):
        if not self._current_hysteresis:
            self._hysteresis_until = ExpiryTime(timedelta(seconds=0))
        else:
            if reset or not self._hysteresis_until:
                self._hysteresis_until = ExpiryTime(self._current_hysteresis)
            else:
                self._hysteresis_until = ExpiryTime(
                    self._current_hysteresis -
                    self._hysteresis_until.remaining())

    def checkSends(self):
        if self.delay.expired():
            self._decrease_hysteresis()
            self._update_remaining_hysteresis_period(reset=True)
            for intent in self._keepIf(lambda M: False):
                self._sender(intent)

    def sendWithHysteresis(self, intent):
        if self._hysteresis_until.expired():
            self._current_hysteresis = self._hysteresis_min_period
            self._sender(intent)
        else:
            dups = self._keepIf(lambda M:
                                (M.targetAddr != intent.targetAddr or
                                 type(M.message) != type(intent.message)))
            # The dups are duplicate sends to the new intent's target;
            # complete them when the actual message is finally sent
            # with the same result
            if dups:
                intent.addCallback(self._dupSentGood(dups),
                                   self._dupSentFail(dups))
            self._hysteresis_queue.append(intent)
            self._increase_hysteresis()
        self._update_remaining_hysteresis_period()

    def cancelSends(self, remoteAddr):
        for each in self._keepIf(lambda M: M.targetAddr != remoteAddr):
            each.tx_done(SendStatus.Failed)

    def _keepIf(self, keepFunc):
        requeues, removes = partition(keepFunc, self._hysteresis_queue)
        self._hysteresis_queue = requeues
        return removes

    @staticmethod
    def _dupSentGood(dups):
        def _finishDups(result, finishedIntent):
            for each in dups:
                each.tx_done(result)
        return _finishDups

    @staticmethod
    def _dupSentFail(dups):
        def _finishDups(result, finishedIntent):
            for each in dups:
                each.tx_done(result)
        return _finishDups
