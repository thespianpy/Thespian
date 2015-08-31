from thespian.actors import ActorAddress
from thespian.system.transport.asyncTransportBase import (asyncTransportBase,
                                                          MAX_PENDING_TRANSMITS)
from thespian.system.transport import TransmitIntent, SendStatus
import unittest


class FakeTransport(asyncTransportBase):

    def __init__(self):
        super(FakeTransport, self).__init__()
        self.intents = []

    def _scheduleTransmitActual(self, intent):
        self.intents.append(intent)

    def serializer(self, intent):
        intent.serialized = True
        return intent

    def forTestingCompleteAPendingIntent(self, result):
        for I in self.intents:
            if I.result is None:
                I.result = result
                I.completionCallback()
                return


class TestAsyncTransportBase(unittest.TestCase):
    scope='unit'

    def setUp(self):
        self.testTrans      = FakeTransport()

    def resetCounters(self):
        self.successCBcalls = 0
        self.failureCBcalls = 0

    def successCB(self, result, arg): self.successCBcalls += 1
    def failureCB(self, result, arg): self.failureCBcalls += 1

    def test_sendIntentToTransport(self):
        testIntent = TransmitIntent(ActorAddress(None), 'message',
                                    self.successCB, self.failureCB)
        self.testTrans.scheduleTransmit(None, testIntent)
        self.assertEqual(1, len(self.testTrans.intents))

    def test_sendIntentToTransportSuccessCallback(self):
        self.resetCounters()
        testIntent = TransmitIntent(ActorAddress(0), 'message',
                                    self.successCB, self.failureCB)
        self.testTrans.scheduleTransmit(None, testIntent)
        self.assertEqual(1, len(self.testTrans.intents))
        self.testTrans.forTestingCompleteAPendingIntent(SendStatus.Sent)
        self.assertEqual(self.successCBcalls, 1)
        self.assertEqual(self.failureCBcalls, 0)

    def test_sendIntentToTransportFailureCallback(self):
        self.resetCounters()
        testIntent = TransmitIntent(ActorAddress(None), 'message',
                                    self.successCB, self.failureCB)
        self.testTrans.scheduleTransmit(None, testIntent)
        self.assertEqual(1, len(self.testTrans.intents))
        self.testTrans.forTestingCompleteAPendingIntent(SendStatus.Failed)
        self.assertEqual(self.successCBcalls, 0)
        self.assertEqual(self.failureCBcalls, 1)

    extraTransmitIds = range(3000, 3003)

    def test_sendIntentToTransportUpToLimitAndThenQueueInternally(self):

        # Initial transmits are all sent directly
        for count in range(MAX_PENDING_TRANSMITS):
            self.testTrans.scheduleTransmit(
                None,
                TransmitIntent(ActorAddress('me'), 'message%d'%count,
                               self.successCB, self.failureCB))
        self.assertEqual(MAX_PENDING_TRANSMITS, len(self.testTrans.intents))

        # After that transmits are queued because none of the previous have completed
        for count in self.extraTransmitIds:
            self.testTrans.scheduleTransmit(
                None,
                TransmitIntent(ActorAddress(9), count,
                               self.successCB, self.failureCB))
        self.assertEqual(MAX_PENDING_TRANSMITS, len(self.testTrans.intents))
        self.assertFalse([I for I in self.testTrans.intents
                          if I.message in self.extraTransmitIds])

    def test_queueSendIntentIsSentOnSuccessCallbackOfPending(self):
        self.resetCounters()
        self.test_sendIntentToTransportUpToLimitAndThenQueueInternally()
        self.testTrans.forTestingCompleteAPendingIntent(SendStatus.Sent)
        self.assertEqual(self.successCBcalls, 1)
        self.assertEqual(self.failureCBcalls, 0)
        self.assertEqual(MAX_PENDING_TRANSMITS+1, len(self.testTrans.intents))
        self.assertEqual(1, len([I for I in self.testTrans.intents
                                 if I.message in self.extraTransmitIds]))

    def test_queueSendIntentIsSentOnFailureCallbackOfPending(self):
        self.resetCounters()
        self.test_sendIntentToTransportUpToLimitAndThenQueueInternally()
        self.testTrans.forTestingCompleteAPendingIntent(SendStatus.Failed)
        self.assertEqual(self.successCBcalls, 0)
        self.assertEqual(self.failureCBcalls, 1)
        self.assertEqual(MAX_PENDING_TRANSMITS+1, len(self.testTrans.intents))
        self.assertEqual(1, len([I for I in self.testTrans.intents
                                 if I.message in self.extraTransmitIds]))

    def test_allQueueSendIntentsAreSentOnEnoughPendingCallbackCompletions(self):
        numExtras = len(self.extraTransmitIds)
        self.resetCounters()
        self.test_sendIntentToTransportUpToLimitAndThenQueueInternally()
        self.assertTrue(MAX_PENDING_TRANSMITS > numExtras)
        for _extras in range(numExtras):
            self.testTrans.forTestingCompleteAPendingIntent(SendStatus.Sent)
        self.assertEqual(self.successCBcalls, numExtras)
        self.assertEqual(MAX_PENDING_TRANSMITS + numExtras, len(self.testTrans.intents))
        self.assertEqual(numExtras, len([I for I in self.testTrans.intents
                                         if I.message in self.extraTransmitIds]))

    def test_extraPendingCallbackCompletionsDoNothing(self):
        numExtras = len(self.extraTransmitIds)
        self.resetCounters()
        self.test_sendIntentToTransportUpToLimitAndThenQueueInternally()
        self.assertTrue(MAX_PENDING_TRANSMITS > numExtras + 3)
        for _extras in range(numExtras + 3):
            self.testTrans.forTestingCompleteAPendingIntent(SendStatus.Sent)
        self.assertEqual(self.successCBcalls, numExtras + 3)
        self.assertEqual(MAX_PENDING_TRANSMITS + numExtras, len(self.testTrans.intents))
        self.assertEqual(numExtras, len([I for I in self.testTrans.intents
                                         if I.message in self.extraTransmitIds]))

    def test_pendingCallbacksClearQueueAndMoreRunsRunAdditionalQueuedOnMoreCompletions(self):
        numExtras = len(self.extraTransmitIds)
        self.test_extraPendingCallbackCompletionsDoNothing()
        # Add more transmits to reach MAX_PENDING_TRANSMITS again
        for _moreExtras in range(numExtras + 3):
            self.testTrans.forTestingCompleteAPendingIntent(SendStatus.Sent)
        expectedCBCount = 2 * (numExtras + 3)
        self.assertEqual(self.successCBcalls, expectedCBCount)
        self.assertEqual(MAX_PENDING_TRANSMITS + numExtras, len(self.testTrans.intents))
        self.assertEqual(numExtras, len([I for I in self.testTrans.intents
                                         if I.message in self.extraTransmitIds]))
        # Now send more and make sure they are queued
        for count in self.extraTransmitIds:
            self.testTrans.scheduleTransmit(
                None,
                TransmitIntent(ActorAddress(3.5), count,
                               self.successCB, self.failureCB))
        self.assertEqual(self.successCBcalls, expectedCBCount)
        self.assertEqual(MAX_PENDING_TRANSMITS + 2 * numExtras, len(self.testTrans.intents))
        self.assertEqual(2 * numExtras, len([I for I in self.testTrans.intents
                                             if I.message in self.extraTransmitIds]))
        # And verify that more completions cause the newly Queued to be run
        for _extras in range(numExtras):
            self.testTrans.forTestingCompleteAPendingIntent(SendStatus.Sent)
        expectedCBCount += numExtras
        self.assertEqual(self.successCBcalls, expectedCBCount)
        self.assertEqual(MAX_PENDING_TRANSMITS + numExtras + numExtras,
                         len(self.testTrans.intents))
        self.assertEqual(2 * numExtras, len([I for I in self.testTrans.intents
                                             if I.message in self.extraTransmitIds]))

    def test_transmitsCompletingWithCallbackDoNotQueue(self):
        self.resetCounters()
        for count in range(MAX_PENDING_TRANSMITS):
            self.testTrans.scheduleTransmit(
                None,
                TransmitIntent(ActorAddress(self), 'message%d'%count,
                               self.successCB, self.failureCB))
            self.testTrans.forTestingCompleteAPendingIntent(SendStatus.Sent)
        self.assertEqual(self.successCBcalls, MAX_PENDING_TRANSMITS)

        numExtras = len(self.extraTransmitIds)
        for count in self.extraTransmitIds:
            self.testTrans.scheduleTransmit(
                None,
                TransmitIntent(ActorAddress(self.testTrans), count,
                               self.successCB, self.failureCB))
            self.testTrans.forTestingCompleteAPendingIntent(SendStatus.Sent)
        self.assertEqual(MAX_PENDING_TRANSMITS + numExtras, len(self.testTrans.intents))
        self.assertEqual(numExtras, len([I for I in self.testTrans.intents
                                         if I.message in self.extraTransmitIds]))
