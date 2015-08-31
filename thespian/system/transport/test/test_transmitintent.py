import unittest
import thespian.test.helpers
from thespian.system.transport import TransmitIntent, SendStatus, MAX_TRANSMIT_RETRIES
from thespian.system.utilis import timePeriodSeconds
from datetime import datetime, timedelta
from time import sleep


class TestSendStatus(unittest.TestCase):
    scope="unit"

    def testSendStatusSuccess(self):
        self.assertEqual(SendStatus.Sent, SendStatus.Sent)
        self.assertTrue(SendStatus.Sent)

    def testSendStatusFailures(self):
        self.assertEqual(SendStatus.Failed, SendStatus.Failed)
        self.assertFalse(SendStatus.Failed)
        self.assertEqual(SendStatus.NotSent, SendStatus.NotSent)
        self.assertFalse(SendStatus.NotSent)
        self.assertEqual(SendStatus.BadPacket, SendStatus.BadPacket)
        self.assertFalse(SendStatus.BadPacket)
        self.assertEqual(SendStatus.DeadTarget, SendStatus.DeadTarget)
        self.assertFalse(SendStatus.DeadTarget)

    def testSendStatusComparisons(self):
        self.assertNotEqual(SendStatus.Sent, SendStatus.Failed)
        self.assertNotEqual(SendStatus.Sent, SendStatus.NotSent)
        self.assertNotEqual(SendStatus.Sent, SendStatus.BadPacket)
        self.assertNotEqual(SendStatus.Sent, SendStatus.DeadTarget)

        self.assertNotEqual(SendStatus.Failed, SendStatus.Sent)
        self.assertNotEqual(SendStatus.Failed, SendStatus.NotSent)
        self.assertNotEqual(SendStatus.Failed, SendStatus.BadPacket)
        self.assertNotEqual(SendStatus.Failed, SendStatus.DeadTarget)

        self.assertNotEqual(SendStatus.NotSent, SendStatus.Failed)
        self.assertNotEqual(SendStatus.NotSent, SendStatus.Sent)
        self.assertNotEqual(SendStatus.NotSent, SendStatus.BadPacket)
        self.assertNotEqual(SendStatus.NotSent, SendStatus.DeadTarget)

        self.assertNotEqual(SendStatus.BadPacket, SendStatus.Failed)
        self.assertNotEqual(SendStatus.BadPacket, SendStatus.NotSent)
        self.assertNotEqual(SendStatus.BadPacket, SendStatus.Sent)
        self.assertNotEqual(SendStatus.BadPacket, SendStatus.DeadTarget)

        self.assertNotEqual(SendStatus.DeadTarget, SendStatus.Failed)
        self.assertNotEqual(SendStatus.DeadTarget, SendStatus.NotSent)
        self.assertNotEqual(SendStatus.DeadTarget, SendStatus.BadPacket)
        self.assertNotEqual(SendStatus.DeadTarget, SendStatus.Sent)


class TestTransmitIntent(unittest.TestCase):
    scope="unit"

    def testNormalTransmit(self):
        ti = TransmitIntent('addr', 'msg')
        self.assertEqual(ti.targetAddr, 'addr')
        self.assertEqual(ti.message, 'msg')
        self.assertEqual(ti.result, None)

    def testNormalTransmitStr(self):
        ti = TransmitIntent('addr', 'msg')
        # Just ensure no exceptions are thrown
        self.assertTrue(str(ti))

    def testNormalTransmitIdentification(self):
        ti = TransmitIntent('addr', 'msg')
        # Just ensure no exceptions are thrown
        self.assertTrue(ti.identify())

    def testNormalTransmitResetAddress(self):
        ti = TransmitIntent('addr', 'msg')
        self.assertEqual(ti.targetAddr, 'addr')
        self.assertEqual(ti.message, 'msg')
        ti.changeTargetAddr('addr2')
        self.assertEqual(ti.targetAddr, 'addr2')
        self.assertEqual(ti.message, 'msg')

    def testNormalTransmitResetMessage(self):
        ti = TransmitIntent('addr', 'msg')
        self.assertEqual(ti.targetAddr, 'addr')
        self.assertEqual(ti.message, 'msg')
        ti.changeMessage('message2')
        self.assertEqual(ti.targetAddr, 'addr')
        self.assertEqual(ti.message, 'message2')

    def testTransmitIntentSetResult(self):
        ti = TransmitIntent('addr', 'msg')
        self.assertEqual(None, ti.result)
        ti.result = SendStatus.Sent
        self.assertEqual(ti.result, SendStatus.Sent)
        ti.result = SendStatus.Failed
        self.assertEqual(ti.result, SendStatus.Failed)

    def testTransmitIntentSetBadResultType(self):
        ti = TransmitIntent('addr', 'msg')
        self.assertEqual(None, ti.result)

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
        ti = TransmitIntent('addr', 'msg', onSuccess = self._success, onError = self._failed)
        ti.result = SendStatus.Sent
        # Ensure no exception thrown
        ti.completionCallback()
        self.assertEqual(self.successes, [(SendStatus.Sent, ti)])
        self.assertEqual(self.failures, [])
        # And again
        ti.completionCallback()
        self.assertEqual(self.successes, [(SendStatus.Sent, ti)])
        self.assertEqual(self.failures, [])

    def testTransmitIntentCallbackFailureNotSentWithTarget(self):
        self.successes = []
        self.failures = []
        ti = TransmitIntent('addr', 'msg', onSuccess = self._success, onError = self._failed)
        ti.result = SendStatus.NotSent
        # Ensure no exception thrown
        ti.completionCallback()
        self.assertEqual(self.successes, [])
        self.assertEqual(self.failures, [(SendStatus.NotSent, ti)])
        # And again
        ti.completionCallback()
        self.assertEqual(self.successes, [])
        self.assertEqual(self.failures, [(SendStatus.NotSent, ti)])

    def testTransmitIntentCallbackFailureFailedWithTarget(self):
        self.successes = []
        self.failures = []
        ti = TransmitIntent('addr', 'msg', onSuccess = self._success, onError = self._failed)
        ti.result = SendStatus.Failed
        # Ensure no exception thrown
        ti.completionCallback()
        self.assertEqual(self.successes, [])
        self.assertEqual(self.failures, [(SendStatus.Failed, ti)])
        # And again
        ti.completionCallback()
        self.assertEqual(self.successes, [])
        self.assertEqual(self.failures, [(SendStatus.Failed, ti)])

    def testTransmitIntentCallbackSuccessWithChainedTargets(self):
        self.successes = []
        self.failures = []
        ti = TransmitIntent('addr', 'msg', onSuccess = self._success, onError = self._failed)
        ti.addCallback(self._success, self._failed)
        ti.result = SendStatus.Sent
        # Ensure no exception thrown
        ti.completionCallback()
        self.assertEqual(self.successes, [(SendStatus.Sent, ti), (SendStatus.Sent, ti)])
        self.assertEqual(self.failures, [])
        # And again
        ti.completionCallback()
        self.assertEqual(self.successes, [(SendStatus.Sent, ti), (SendStatus.Sent, ti)])
        self.assertEqual(self.failures, [])

    def testTransmitIntentCallbackFailureNotSentWithChainedTargets(self):
        self.successes = []
        self.failures = []
        ti = TransmitIntent('addr', 'msg', onSuccess = self._success, onError = self._failed)
        ti.addCallback(self._success, self._failed)
        ti.result = SendStatus.NotSent
        # Ensure no exception thrown
        ti.completionCallback()
        self.assertEqual(self.successes, [])
        self.assertEqual(self.failures, [(SendStatus.NotSent, ti), (SendStatus.NotSent, ti)])
        # And again
        ti.completionCallback()
        self.assertEqual(self.successes, [])
        self.assertEqual(self.failures, [(SendStatus.NotSent, ti), (SendStatus.NotSent, ti)])

    def testTransmitIntentCallbackFailureFailedWithChainedTargets(self):
        self.successes = []
        self.failures = []
        ti = TransmitIntent('addr', 'msg', onSuccess = self._success, onError = self._failed)
        ti.addCallback(self._success, self._failed)
        ti.result = SendStatus.Failed
        # Ensure no exception thrown
        ti.completionCallback()
        self.assertEqual(self.successes, [])
        self.assertEqual(self.failures, [(SendStatus.Failed, ti), (SendStatus.Failed, ti)])
        # And again
        ti.completionCallback()
        self.assertEqual(self.successes, [])
        self.assertEqual(self.failures, [(SendStatus.Failed, ti), (SendStatus.Failed, ti)])

    def testTransmitIntentCallbackSuccessWithChangedTargetsAdded(self):
        self.successes = []
        self.failures = []
        ti = TransmitIntent('addr', 'msg', onSuccess = self._success, onError = self._failed)
        ti.result = SendStatus.Sent
        # Ensure no exception thrown
        ti.completionCallback()
        self.assertEqual(self.successes, [(SendStatus.Sent, ti)])
        self.assertEqual(self.failures, [])
        # And again
        ti.addCallback(self._success, self._failed)
        ti.completionCallback()
        self.assertEqual(self.successes, [(SendStatus.Sent, ti), (SendStatus.Sent, ti)])
        self.assertEqual(self.failures, [])

    def testTransmitIntentCallbackFailureNotSentWithChangedTargetsAdded(self):
        self.successes = []
        self.failures = []
        ti = TransmitIntent('addr', 'msg', onSuccess = self._success, onError = self._failed)
        ti.result = SendStatus.NotSent
        # Ensure no exception thrown
        ti.completionCallback()
        self.assertEqual(self.successes, [])
        self.assertEqual(self.failures, [(SendStatus.NotSent, ti)])
        # And again
        ti.addCallback(self._success, self._failed)
        ti.completionCallback()
        self.assertEqual(self.successes, [])
        self.assertEqual(self.failures, [(SendStatus.NotSent, ti), (SendStatus.NotSent, ti)])

    def testTransmitIntentCallbackFailureFailedWithChangedTargetsAdded(self):
        self.successes = []
        self.failures = []
        ti = TransmitIntent('addr', 'msg', onSuccess = self._success, onError = self._failed)
        ti.result = SendStatus.Failed
        # Ensure no exception thrown
        ti.completionCallback()
        self.assertEqual(self.successes, [])
        self.assertEqual(self.failures, [(SendStatus.Failed, ti)])
        # And again
        ti.addCallback(self._success, self._failed)
        ti.completionCallback()
        self.assertEqual(self.successes, [])
        self.assertEqual(self.failures, [(SendStatus.Failed, ti), (SendStatus.Failed, ti)])

    def testTransmitIntentRetry(self):
        ti = TransmitIntent('addr', 'msg')
        for x in range(MAX_TRANSMIT_RETRIES+1):
            self.assertTrue(ti.retry())
        self.assertFalse(ti.retry())

    def testTransmitIntentRetryTiming(self):
        maxPeriod = timedelta(milliseconds=90)
        period = timedelta(milliseconds=30)
        ti = TransmitIntent('addr', 'msg', maxPeriod=maxPeriod, retryPeriod=period)
        self.assertFalse(ti.timeToRetry())
        sleep(timePeriodSeconds(period))
        self.assertFalse(ti.timeToRetry())

        self.assertTrue(ti.retry())
        self.assertFalse(ti.timeToRetry())
        sleep(timePeriodSeconds(period))
        self.assertTrue(ti.timeToRetry())

        self.assertTrue(ti.retry())
        self.assertFalse(ti.timeToRetry())
        sleep(timePeriodSeconds(period))
        self.assertFalse(ti.timeToRetry())  # Each retry increases
        sleep(timePeriodSeconds(period))
        self.assertTrue(ti.timeToRetry())

        self.assertFalse(ti.retry())  # Exceeds maximum time

    def testTransmitIntentRetryTimingExceedsLimit(self):
        maxPeriod = timedelta(seconds=90)
        period = timedelta(microseconds=1)
        ti = TransmitIntent('addr', 'msg', maxPeriod=maxPeriod, retryPeriod=period)
        self.assertFalse(ti.timeToRetry())

        for N in range(MAX_TRANSMIT_RETRIES+1):
            self.assertTrue(ti.retry())
            for x in range(90):
                if ti.timeToRetry(): break
                sleep(timePeriodSeconds(period))
            self.assertTrue(ti.timeToRetry())

        self.assertFalse(ti.retry())

    def testTransmitIntentDelay(self):
        maxPeriod = timedelta(milliseconds=90)
        period = timedelta(milliseconds=30)
        ti = TransmitIntent('addr', 'msg', maxPeriod=maxPeriod, retryPeriod=period)
        delay = ti.delay()
        self.assertGreater(delay, timedelta(milliseconds=88))
        self.assertLess(delay, timedelta(milliseconds=91))

    def testTransmitIntentRetryDelay(self):
        maxPeriod = timedelta(milliseconds=90)
        period = timedelta(milliseconds=30)
        ti = TransmitIntent('addr', 'msg', maxPeriod=maxPeriod, retryPeriod=period)
        ti.retry()
        delay = ti.delay()
        self.assertGreater(delay, timedelta(milliseconds=28))
        self.assertLess(delay, timedelta(milliseconds=31))

    def testTransmitIntentRetryRetryDelay(self):
        maxPeriod = timedelta(milliseconds=90)
        period = timedelta(milliseconds=30)
        ti = TransmitIntent('addr', 'msg', maxPeriod=maxPeriod, retryPeriod=period)
        ti.retry()
        ti.retry()
        delay = ti.delay()
        self.assertGreater(delay, timedelta(milliseconds=58))
        self.assertLess(delay, timedelta(milliseconds=61))

