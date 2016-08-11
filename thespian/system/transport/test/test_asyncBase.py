from thespian.actors import ActorAddress
from thespian.system.transport.asyncTransportBase import (asyncTransportBase,
                                                          MAX_PENDING_TRANSMITS)
from thespian.system.transport import TransmitIntent, SendStatus


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
                I.tx_done(result)
                return


class TestUnitAsyncTransportBase(object):

    def resetCounters(self):
        self.successCBcalls = 0
        self.failureCBcalls = 0

    def successCB(self, result, arg): self.successCBcalls += 1
    def failureCB(self, result, arg): self.failureCBcalls += 1

    def test_sendIntentToTransport(self):
        testTrans = FakeTransport()
        testIntent = TransmitIntent(ActorAddress(None), 'message',
                                    self.successCB, self.failureCB)
        testTrans.scheduleTransmit(None, testIntent)
        assert 1 == len(testTrans.intents)

    def test_sendIntentToTransportSuccessCallback(self):
        testTrans = FakeTransport()
        self.resetCounters()
        testIntent = TransmitIntent(ActorAddress(0), 'message',
                                    self.successCB, self.failureCB)
        testTrans.scheduleTransmit(None, testIntent)
        assert 1 == len(testTrans.intents)
        testTrans.forTestingCompleteAPendingIntent(SendStatus.Sent)
        assert self.successCBcalls == 1
        assert self.failureCBcalls == 0

    def test_sendIntentToTransportFailureCallback(self):
        testTrans = FakeTransport()
        self.resetCounters()
        testIntent = TransmitIntent(ActorAddress(None), 'message',
                                    self.successCB, self.failureCB)
        testTrans.scheduleTransmit(None, testIntent)
        assert 1 == len(testTrans.intents)
        testTrans.forTestingCompleteAPendingIntent(SendStatus.Failed)
        assert self.successCBcalls == 0
        assert self.failureCBcalls == 1

    extraTransmitIds = range(3000, 3003)

    def _sendAndQueue(self, testTrans):
        # Initial transmits are all sent directly
        for count in range(MAX_PENDING_TRANSMITS):
            testTrans.scheduleTransmit(
                None,
                TransmitIntent(ActorAddress('me'), 'message%d'%count,
                               self.successCB, self.failureCB))
        assert MAX_PENDING_TRANSMITS == len(testTrans.intents)

        # After that transmits are queued because none of the previous have completed
        for count in self.extraTransmitIds:
            testTrans.scheduleTransmit(
                None,
                TransmitIntent(ActorAddress(9), count,
                               self.successCB, self.failureCB))
        assert MAX_PENDING_TRANSMITS == len(testTrans.intents)
        assert not [I for I in testTrans.intents
                    if I.message in self.extraTransmitIds]


    def test_sendIntentToTransportUpToLimitAndThenQueueInternally(self):
        testTrans = FakeTransport()
        self._sendAndQueue(testTrans)

    def test_queueSendIntentIsSentOnSuccessCallbackOfPending(self):
        testTrans = FakeTransport()
        self.resetCounters()
        self._sendAndQueue(testTrans)
        testTrans.forTestingCompleteAPendingIntent(SendStatus.Sent)
        assert self.successCBcalls == 1
        assert self.failureCBcalls == 0
        assert MAX_PENDING_TRANSMITS+1 == len(testTrans.intents)
        assert 1 == len([I for I in testTrans.intents
                         if I.message in self.extraTransmitIds])

    def test_queueSendIntentIsSentOnFailureCallbackOfPending(self):
        testTrans = FakeTransport()
        self.resetCounters()
        self._sendAndQueue(testTrans)
        testTrans.forTestingCompleteAPendingIntent(SendStatus.Failed)
        assert self.successCBcalls == 0
        assert self.failureCBcalls == 1
        assert MAX_PENDING_TRANSMITS+1 == len(testTrans.intents)
        assert 1 == len([I for I in testTrans.intents
                         if I.message in self.extraTransmitIds])

    def test_allQueueSendIntentsAreSentOnEnoughPendingCallbackCompletions(self):
        testTrans = FakeTransport()
        numExtras = len(self.extraTransmitIds)
        self.resetCounters()
        self._sendAndQueue(testTrans)
        assert MAX_PENDING_TRANSMITS > numExtras
        for _extras in range(numExtras):
            testTrans.forTestingCompleteAPendingIntent(SendStatus.Sent)
        assert self.successCBcalls == numExtras
        assert MAX_PENDING_TRANSMITS + numExtras == len(testTrans.intents)
        assert numExtras == len([I for I in testTrans.intents
                                 if I.message in self.extraTransmitIds])

    def test_extraPendingCallbackCompletionsDoNothing(self):
        self._extraCompletions(FakeTransport())

    def _extraCompletions(self, testTrans):
        numExtras = len(self.extraTransmitIds)
        self.resetCounters()
        self._sendAndQueue(testTrans)
        assert MAX_PENDING_TRANSMITS > numExtras + 3
        for _extras in range(numExtras + 3):
            testTrans.forTestingCompleteAPendingIntent(SendStatus.Sent)
        assert self.successCBcalls == numExtras + 3
        assert MAX_PENDING_TRANSMITS + numExtras == len(testTrans.intents)
        assert numExtras == len([I for I in testTrans.intents
                                 if I.message in self.extraTransmitIds])

    def test_pendingCallbacksClearQueueAndMoreRunsRunAdditionalQueuedOnMoreCompletions(self):
        testTrans = FakeTransport()
        numExtras = len(self.extraTransmitIds)
        self._extraCompletions(testTrans)
        # Add more transmits to reach MAX_PENDING_TRANSMITS again
        for _moreExtras in range(numExtras + 3):
            testTrans.forTestingCompleteAPendingIntent(SendStatus.Sent)
        expectedCBCount = 2 * (numExtras + 3)
        assert self.successCBcalls == expectedCBCount
        assert MAX_PENDING_TRANSMITS + numExtras == len(testTrans.intents)
        assert numExtras == len([I for I in testTrans.intents
                                 if I.message in self.extraTransmitIds])
        # Now send more and make sure they are queued
        for count in self.extraTransmitIds:
            testTrans.scheduleTransmit(
                None,
                TransmitIntent(ActorAddress(3.5), count,
                               self.successCB, self.failureCB))
        assert self.successCBcalls == expectedCBCount
        assert MAX_PENDING_TRANSMITS + 2 * numExtras == len(testTrans.intents)
        assert 2 * numExtras == len([I for I in testTrans.intents
                                     if I.message in self.extraTransmitIds])
        # And verify that more completions cause the newly Queued to be run
        for _extras in range(numExtras):
            testTrans.forTestingCompleteAPendingIntent(SendStatus.Sent)
        expectedCBCount += numExtras
        assert self.successCBcalls == expectedCBCount
        assert (MAX_PENDING_TRANSMITS +
                numExtras +
                numExtras) == len(testTrans.intents)
        assert 2 * numExtras == len([I for I in testTrans.intents
                                     if I.message in self.extraTransmitIds])

    def test_transmitsCompletingWithCallbackDoNotQueue(self):
        testTrans = FakeTransport()
        self.resetCounters()
        for count in range(MAX_PENDING_TRANSMITS):
            testTrans.scheduleTransmit(
                None,
                TransmitIntent(ActorAddress(self), 'message%d'%count,
                               self.successCB, self.failureCB))
            testTrans.forTestingCompleteAPendingIntent(SendStatus.Sent)
        assert self.successCBcalls == MAX_PENDING_TRANSMITS

        numExtras = len(self.extraTransmitIds)
        for count in self.extraTransmitIds:
            testTrans.scheduleTransmit(
                None,
                TransmitIntent(ActorAddress(testTrans), count,
                               self.successCB, self.failureCB))
            testTrans.forTestingCompleteAPendingIntent(SendStatus.Sent)
        assert MAX_PENDING_TRANSMITS + numExtras == len(testTrans.intents)
        assert numExtras == len([I for I in testTrans.intents
                                 if I.message in self.extraTransmitIds])
