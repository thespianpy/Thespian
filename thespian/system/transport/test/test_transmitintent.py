from thespian.system.transport import (TransmitIntent, SendStatus,
                                       MAX_TRANSMIT_RETRIES)
from thespian.system.timing import timePeriodSeconds
from datetime import timedelta
from pytest import mark
try: from unittest.mock import patch
except ImportError:
    try: from mock import patch
    except ImportError:
        patch = None
from contextlib import contextmanager

@contextmanager
def update_elapsed_time(time_base, elapsed):
    with patch('thespian.system.timing.currentTime') as p_ctime:
        p_ctime.return_value = time_base + (timePeriodSeconds(elapsed)
                                            if isinstance(elapsed, timedelta)
                                            else elapsed)
        yield p_ctime.return_value



class TestUnitSendStatus(object):

    def testSendStatusSuccess(self):
        assert SendStatus.Sent == SendStatus.Sent
        assert SendStatus.Sent

    def testSendStatusFailures(self):
        assert SendStatus.Failed == SendStatus.Failed
        assert not SendStatus.Failed
        assert SendStatus.NotSent == SendStatus.NotSent
        assert not SendStatus.NotSent
        assert SendStatus.BadPacket == SendStatus.BadPacket
        assert not SendStatus.BadPacket
        assert SendStatus.DeadTarget == SendStatus.DeadTarget
        assert not SendStatus.DeadTarget

    def testSendStatusComparisons(self):
        assert SendStatus.Sent != SendStatus.Failed
        assert SendStatus.Sent != SendStatus.NotSent
        assert SendStatus.Sent != SendStatus.BadPacket
        assert SendStatus.Sent != SendStatus.DeadTarget

        assert SendStatus.Failed != SendStatus.Sent
        assert SendStatus.Failed != SendStatus.NotSent
        assert SendStatus.Failed != SendStatus.BadPacket
        assert SendStatus.Failed != SendStatus.DeadTarget

        assert SendStatus.NotSent != SendStatus.Failed
        assert SendStatus.NotSent != SendStatus.Sent
        assert SendStatus.NotSent != SendStatus.BadPacket
        assert SendStatus.NotSent != SendStatus.DeadTarget

        assert SendStatus.BadPacket != SendStatus.Failed
        assert SendStatus.BadPacket != SendStatus.NotSent
        assert SendStatus.BadPacket != SendStatus.Sent
        assert SendStatus.BadPacket != SendStatus.DeadTarget

        assert SendStatus.DeadTarget != SendStatus.Failed
        assert SendStatus.DeadTarget != SendStatus.NotSent
        assert SendStatus.DeadTarget != SendStatus.BadPacket
        assert SendStatus.DeadTarget != SendStatus.Sent


class TestUnitTransmitIntent(object):

    def testNormalTransmit(self):
        ti = TransmitIntent('addr', 'msg')
        assert ti.targetAddr == 'addr'
        assert ti.message == 'msg'
        assert ti.result == None

    def testNormalTransmitStr(self):
        ti = TransmitIntent('addr', 'msg')
        # Just ensure no exceptions are thrown
        assert str(ti)

    def testNormalTransmitIdentification(self):
        ti = TransmitIntent('addr', 'msg')
        # Just ensure no exceptions are thrown
        assert ti.identify()

    def testNormalTransmitResetAddress(self):
        ti = TransmitIntent('addr', 'msg')
        assert ti.targetAddr == 'addr'
        assert ti.message == 'msg'
        ti.changeTargetAddr('addr2')
        assert ti.targetAddr == 'addr2'
        assert ti.message == 'msg'

    def testNormalTransmitResetMessage(self):
        ti = TransmitIntent('addr', 'msg')
        assert ti.targetAddr == 'addr'
        assert ti.message == 'msg'
        ti.changeMessage('message2')
        assert ti.targetAddr == 'addr'
        assert ti.message == 'message2'

    def testTransmitIntentSetResult(self):
        ti = TransmitIntent('addr', 'msg')
        assert None == ti.result
        ti.result = SendStatus.Sent
        assert ti.result == SendStatus.Sent
        ti.result = SendStatus.Failed
        assert ti.result == SendStatus.Failed

    def testTransmitIntentSetBadResultType(self):
        ti = TransmitIntent('addr', 'msg')
        assert None == ti.result

    def _success(self, result, intent):
        self.successes.append( (result, intent) )
    def _failed(self, result, intent):
        self.failures.append( (result, intent) )

    def testTransmitIntentCallbackSuccess(self):
        ti = TransmitIntent('addr', 'msg')
        ti.result = SendStatus.Sent
        # Ensure no exception thrown
        ti.completionCallback()
        # And again
        ti.completionCallback()

    def testTransmitIntentCallbackFailureNotSent(self):
        ti = TransmitIntent('addr', 'msg')
        ti.result = SendStatus.NotSent
        # Ensure no exception thrown
        ti.completionCallback()
        # And again
        ti.completionCallback()

    def testTransmitIntentCallbackFailureFailed(self):
        ti = TransmitIntent('addr', 'msg')
        ti.result = SendStatus.Failed
        # Ensure no exception thrown
        ti.completionCallback()
        # And again
        ti.completionCallback()

    def testTransmitIntentCallbackSuccessWithTarget(self):
        self.successes = []
        self.failures = []
        ti = TransmitIntent('addr', 'msg',
                            onSuccess = self._success,
                            onError = self._failed)
        ti.result = SendStatus.Sent
        # Ensure no exception thrown
        ti.completionCallback()
        assert self.successes == [(SendStatus.Sent, ti)]
        assert self.failures == []
        # And again
        ti.completionCallback()
        assert self.successes == [(SendStatus.Sent, ti)]
        assert self.failures == []

    def testTransmitIntentCallbackFailureNotSentWithTarget(self):
        self.successes = []
        self.failures = []
        ti = TransmitIntent('addr', 'msg',
                            onSuccess = self._success,
                            onError = self._failed)
        ti.result = SendStatus.NotSent
        # Ensure no exception thrown
        ti.completionCallback()
        assert self.successes == []
        assert self.failures == [(SendStatus.NotSent, ti)]
        # And again
        ti.completionCallback()
        assert self.successes == []
        assert self.failures == [(SendStatus.NotSent, ti)]

    def testTransmitIntentCallbackFailureFailedWithTarget(self):
        self.successes = []
        self.failures = []
        ti = TransmitIntent('addr', 'msg',
                            onSuccess = self._success,
                            onError = self._failed)
        ti.result = SendStatus.Failed
        # Ensure no exception thrown
        ti.completionCallback()
        assert self.successes == []
        assert self.failures == [(SendStatus.Failed, ti)]
        # And again
        ti.completionCallback()
        assert self.successes == []
        assert self.failures == [(SendStatus.Failed, ti)]

    def testTransmitIntentCallbackSuccessWithChainedTargets(self):
        self.successes = []
        self.failures = []
        ti = TransmitIntent('addr', 'msg',
                            onSuccess = self._success,
                            onError = self._failed)
        ti.addCallback(self._success, self._failed)
        ti.result = SendStatus.Sent
        # Ensure no exception thrown
        ti.completionCallback()
        assert self.successes == [(SendStatus.Sent, ti), (SendStatus.Sent, ti)]
        assert self.failures == []
        # And again
        ti.completionCallback()
        assert self.successes == [(SendStatus.Sent, ti), (SendStatus.Sent, ti)]
        assert self.failures == []

    def testTransmitIntentCallbackFailureNotSentWithChainedTargets(self):
        self.successes = []
        self.failures = []
        ti = TransmitIntent('addr', 'msg',
                            onSuccess = self._success,
                            onError = self._failed)
        ti.addCallback(self._success, self._failed)
        ti.result = SendStatus.NotSent
        # Ensure no exception thrown
        ti.completionCallback()
        assert self.successes == []
        assert self.failures == [(SendStatus.NotSent, ti),
                                 (SendStatus.NotSent, ti)]
        # And again
        ti.completionCallback()
        assert self.successes == []
        assert self.failures == [(SendStatus.NotSent, ti),
                                 (SendStatus.NotSent, ti)]

    def testTransmitIntentCallbackFailureFailedWithChainedTargets(self):
        self.successes = []
        self.failures = []
        ti = TransmitIntent('addr', 'msg',
                            onSuccess = self._success,
                            onError = self._failed)
        ti.addCallback(self._success, self._failed)
        ti.result = SendStatus.Failed
        # Ensure no exception thrown
        ti.completionCallback()
        assert self.successes == []
        assert self.failures == [(SendStatus.Failed, ti),
                                 (SendStatus.Failed, ti)]
        # And again
        ti.completionCallback()
        assert self.successes == []
        assert self.failures == [(SendStatus.Failed, ti),
                                 (SendStatus.Failed, ti)]

    def testTransmitIntentCallbackSuccessWithChangedTargetsAdded(self):
        self.successes = []
        self.failures = []
        ti = TransmitIntent('addr', 'msg',
                            onSuccess = self._success,
                            onError = self._failed)
        ti.result = SendStatus.Sent
        # Ensure no exception thrown
        ti.completionCallback()
        assert self.successes == [(SendStatus.Sent, ti)]
        assert self.failures == []
        # And again
        ti.addCallback(self._success, self._failed)
        ti.completionCallback()
        assert self.successes == [(SendStatus.Sent, ti), (SendStatus.Sent, ti)]
        assert self.failures == []

    def testTransmitIntentCallbackFailureNotSentWithChangedTargetsAdded(self):
        self.successes = []
        self.failures = []
        ti = TransmitIntent('addr', 'msg',
                            onSuccess = self._success,
                            onError = self._failed)
        ti.result = SendStatus.NotSent
        # Ensure no exception thrown
        ti.completionCallback()
        assert self.successes == []
        assert self.failures == [(SendStatus.NotSent, ti)]
        # And again
        ti.addCallback(self._success, self._failed)
        ti.completionCallback()
        assert self.successes == []
        assert self.failures == [(SendStatus.NotSent, ti),
                                 (SendStatus.NotSent, ti)]

    def testTransmitIntentCallbackFailureFailedWithChangedTargetsAdded(self):
        self.successes = []
        self.failures = []
        ti = TransmitIntent('addr', 'msg',
                            onSuccess = self._success,
                            onError = self._failed)
        ti.result = SendStatus.Failed
        # Ensure no exception thrown
        ti.completionCallback()
        assert self.successes == []
        assert self.failures == [(SendStatus.Failed, ti)]
        # And again
        ti.addCallback(self._success, self._failed)
        ti.completionCallback()
        assert self.successes == []
        assert self.failures == [(SendStatus.Failed, ti),
                                 (SendStatus.Failed, ti)]

    def testTransmitIntentRetry(self):
        ti = TransmitIntent('addr', 'msg')
        for x in range(MAX_TRANSMIT_RETRIES+1):
            assert ti.retry()
        assert not ti.retry()

    @mark.skipif(not patch, reason='requires mock patch')
    def testTransmitIntentRetryTiming(self):
        maxPeriod = timedelta(milliseconds=90)
        period = timedelta(milliseconds=30)
        now = 0.01
        timepad = timedelta(microseconds=10) # avoid float imprecision
        with update_elapsed_time(now, timedelta(0)):
            ti = TransmitIntent('addr', 'msg',
                                maxPeriod=maxPeriod,
                                retryPeriod=period)
            assert not ti.timeToRetry()

        with update_elapsed_time(now, period + timepad):
            assert not ti.timeToRetry()

            assert ti.retry()
            assert not ti.timeToRetry()

        with update_elapsed_time(now, period + period + timepad):
            assert ti.timeToRetry()
            assert ti.retry()
            assert not ti.timeToRetry()

        with update_elapsed_time(now, period * 3 + timepad):
            assert not ti.timeToRetry()  # Each retry increases

        with update_elapsed_time(now, period * 4 + timepad):
            assert ti.timeToRetry()
            assert not ti.retry()  # Exceeds maximum time

    @mark.skipif(not patch, reason='requires mock patch')
    def testTransmitIntentRetryTimingExceedsLimit(self):
        maxPeriod = timedelta(seconds=90)
        period = timedelta(microseconds=1)
        now = 1.23
        timepad = timedelta(microseconds=10) # avoid float imprecision
        with update_elapsed_time(now, timedelta(0)):
            ti = TransmitIntent('addr', 'msg',
                                maxPeriod=maxPeriod,
                                retryPeriod=period)
            assert not ti.timeToRetry()

        timeoffset = timedelta(0)
        for N in range(MAX_TRANSMIT_RETRIES+1):
            # Indicate "failure" and the need to retry
            with update_elapsed_time(now, timeoffset + timepad):
                assert ti.retry()
            # Wait for the indication that it is time to retry
            time_to_retry = False
            for x in range(90):
                with update_elapsed_time(now, timeoffset + timepad):
                    # Only call timeToRetry once, because it auto-resets
                    time_to_retry = ti.timeToRetry()
                    if time_to_retry: break
                timeoffset += (period + (period / 2))
                # = period * 1.5, but python2 cannot multiply
                # timedelta by fractions.
            assert time_to_retry

        with update_elapsed_time(now, timeoffset + timepad):
            assert not ti.retry()

    def testTransmitIntentDelay(self):
        maxPeriod = timedelta(milliseconds=90)
        period = timedelta(milliseconds=30)
        ti = TransmitIntent('addr', 'msg',
                            maxPeriod=maxPeriod,
                            retryPeriod=period)
        delay = ti.delay()
        assert delay > timedelta(milliseconds=88)
        assert delay < timedelta(milliseconds=91)

    def testTransmitIntentRetryDelay(self):
        maxPeriod = timedelta(milliseconds=90)
        period = timedelta(milliseconds=30)
        ti = TransmitIntent('addr', 'msg',
                            maxPeriod=maxPeriod,
                            retryPeriod=period)
        ti.retry()
        delay = ti.delay()
        assert delay > timedelta(milliseconds=28)
        assert delay < timedelta(milliseconds=31)

    def testTransmitIntentRetryRetryDelay(self):
        maxPeriod = timedelta(milliseconds=90)
        period = timedelta(milliseconds=30)
        ti = TransmitIntent('addr', 'msg',
                            maxPeriod=maxPeriod,
                            retryPeriod=period)
        ti.retry()
        ti.retry()
        delay = ti.delay()
        assert delay > timedelta(milliseconds=58)
        assert delay < timedelta(milliseconds=61)
