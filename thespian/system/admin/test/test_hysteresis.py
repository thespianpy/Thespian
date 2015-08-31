import unittest
import thespian.test.helpers
from thespian.system.admin.convention import HysteresisDelaySender
from thespian.system.transport import TransmitIntent, SendStatus
from datetime import datetime, timedelta
from time import sleep


class TestHysteresis(unittest.TestCase):
    scope="unit"

    def send(self, intent):
        if not hasattr(self, 'sends'): self.sends = []
        self.sends.append(intent)
        intent.result = SendStatus.Sent
        intent.completionCallback()

    def successfulIntent(self, err, intent):
        if not hasattr(self, 'successes'): self.successes = []
        self.successes.append(intent)

    def failedIntent(self, err, intent):
        if not hasattr(self, 'fails'): self.fails = []
        self.fails.append(intent)

    def testSingleSend(self):
        self.sends = []
        hs = HysteresisDelaySender(self.send)
        targetAddr = 'addr'
        msg = 'msg'
        intent = TransmitIntent(targetAddr, msg)
        hs.sendWithHysteresis(intent)
        # Should have been sent immediately
        self.assertEqual(1, len(getattr(self, 'sends', [])))

    def testTwoSends(self):
        self.sends = []
        hs = HysteresisDelaySender(self.send,
                                   hysteresis_min_period = timedelta(milliseconds=2),
                                   hysteresis_max_period = timedelta(milliseconds=10),
                                   hysteresis_rate = 2)
        intent1 = TransmitIntent('addr1', 'msg1')
        intent2 = TransmitIntent('addr1', 'msg2')
        hs.sendWithHysteresis(intent1)
        hs.sendWithHysteresis(intent2)
        # First was sent immediately, second is delayed
        self.assertEqual(1, len(getattr(self, 'sends', [])))
        self.assertEqual(intent1, self.sends[0])
        self.assertNotEqual(timedelta(seconds=0), hs.delay.remaining())
        self.assertGreaterEqual(timedelta(milliseconds=10), hs.delay.remaining())
        delayTime = hs.delay.remainingSeconds()
        print('Remaining seconds: %s (%s)'%(delayTime, type(delayTime)))
        sleep(delayTime)
        hs.checkSends()
        self.assertEqual(2, len(getattr(self, 'sends', [])))
        self.assertEqual(intent1, self.sends[0])
        self.assertEqual(intent2, self.sends[1])
        # Ensure that there are no more send attempts
        sleep(hs.delay.remainingSeconds())
        self.assertEqual(2, len(getattr(self, 'sends', [])))
        sleep(delayTime)
        self.assertEqual(2, len(getattr(self, 'sends', [])))

    def testTwoSendsIntentTimeoutIgnored(self):
        self.sends = []
        hs = HysteresisDelaySender(self.send,
                                   hysteresis_min_period = timedelta(milliseconds=100),
                                   hysteresis_max_period = timedelta(milliseconds=110),
                                   hysteresis_rate = 2)
        intent1 = TransmitIntent('addr1', 'msg1')
        intent2 = TransmitIntent('addr1', 'msg2', maxPeriod=timedelta(milliseconds=10))
        hs.sendWithHysteresis(intent1)
        hs.sendWithHysteresis(intent2)
        # First was sent immediately, second is delayed
        self.assertEqual(1, len(getattr(self, 'sends', [])))
        self.assertEqual(intent1, self.sends[0])
        self.assertNotEqual(timedelta(seconds=0), hs.delay.remaining())
        self.assertGreaterEqual(timedelta(milliseconds=110), hs.delay.remaining())
        self.assertLess(timedelta(milliseconds=95), hs.delay.remaining())
        print('Remaining seconds: %s (%s)'%(hs.delay.remainingSeconds(),
                                            type(hs.delay.remainingSeconds())))
        sleep(hs.delay.remainingSeconds())
        hs.checkSends()
        self.assertEqual(2, len(getattr(self, 'sends', [])))
        self.assertEqual(intent1, self.sends[0])
        self.assertEqual(intent2, self.sends[1])

    def testTwentySendsDifferentAddresses(self):
        self.sends = []
        hs = HysteresisDelaySender(self.send,
                                   hysteresis_min_period = timedelta(milliseconds=2),
                                   hysteresis_max_period = timedelta(milliseconds=10),
                                   hysteresis_rate = 2)
        intents = [TransmitIntent('addr1', 'msg1')]
        for num in range(20):
            intents.append(TransmitIntent('addr%d'%num, 'msg'))
        for each in intents:
            hs.sendWithHysteresis(each)
        # First was sent immediately, all others are delayed
        self.assertEqual(1, len(getattr(self, 'sends', [])))
        self.assertEqual(intents[0], self.sends[0])
        self.assertNotEqual(timedelta(seconds=0), hs.delay.remaining())
        self.assertGreaterEqual(timedelta(milliseconds=10), hs.delay.remaining())
        self.assertLess(timedelta(milliseconds=9), hs.delay.remaining())
        delayTime = hs.delay.remainingSeconds()
        print('Remaining seconds: %s (%s)'%(delayTime, type(delayTime)))
        sleep(delayTime)
        hs.checkSends()
        self.assertEqual(len(intents), len(getattr(self, 'sends', [])))
        for num in range(len(intents)):
            self.assertEqual(intents[num], self.sends[num])
        # Ensure that there are no more send attempts
        sleep(hs.delay.remainingSeconds())
        self.assertEqual(len(intents), len(getattr(self, 'sends', [])))
        sleep(delayTime)
        self.assertEqual(len(intents), len(getattr(self, 'sends', [])))

    def testTwentySendsDifferentAddressesCancelOne(self):
        self.sends = []
        hs = HysteresisDelaySender(self.send,
                                   hysteresis_min_period = timedelta(milliseconds=2),
                                   hysteresis_max_period = timedelta(milliseconds=10),
                                   hysteresis_rate = 2)
        intents = [TransmitIntent('addr1', 'msg1')]
        for num in range(20):
            intents.append(TransmitIntent('addr%d'%num, 'msg'))
        for each in intents:
            hs.sendWithHysteresis(each)
        # First was sent immediately, all others are delayed
        self.assertEqual(1, len(getattr(self, 'sends', [])))
        self.assertEqual(intents[0], self.sends[0])
        self.assertNotEqual(timedelta(seconds=0), hs.delay.remaining())
        self.assertGreaterEqual(timedelta(milliseconds=10), hs.delay.remaining())
        self.assertLess(timedelta(milliseconds=9), hs.delay.remaining())
        hs.cancelSends('addr10')
        delayTime = hs.delay.remainingSeconds()
        print('Remaining seconds: %s (%s)'%(delayTime, type(delayTime)))
        sleep(delayTime)
        hs.checkSends()
        self.assertEqual(len(intents)-1, len(getattr(self, 'sends', [])))
        adj = 0
        for num in range(len(intents)):
            if num == 11:
                adj = 1
                continue
            self.assertEqual(intents[num], self.sends[num - adj])
        # Ensure that there are no more send attempts
        sleep(hs.delay.remainingSeconds())
        self.assertEqual(len(intents)-1, len(getattr(self, 'sends', [])))
        sleep(delayTime)
        self.assertEqual(len(intents)-1, len(getattr(self, 'sends', [])))

    def testTwentySendsSameAddressSameMessageTypeSuccess(self):
        self.sends = []
        self.fails = []
        hs = HysteresisDelaySender(self.send,
                                   hysteresis_min_period = timedelta(milliseconds=2),
                                   hysteresis_max_period = timedelta(milliseconds=10),
                                   hysteresis_rate = 2)
        intents = [TransmitIntent('addr1', 'msg1')]
        for num in range(20):
            intents.append(TransmitIntent('addr1', 'msg',
                                          onSuccess = self.successfulIntent,
                                          onError = self.failedIntent))
        for each in intents:
            hs.sendWithHysteresis(each)
        # First was sent immediately, all others are delayed
        self.assertEqual(1, len(getattr(self, 'sends', [])))
        self.assertEqual(intents[0], self.sends[0])
        self.assertNotEqual(timedelta(seconds=0), hs.delay.remaining())
        self.assertGreaterEqual(timedelta(milliseconds=10), hs.delay.remaining())
        self.assertLess(timedelta(milliseconds=9), hs.delay.remaining())
        print('Remaining seconds: %s (%s)'%(hs.delay.remainingSeconds(),
                                            type(hs.delay.remainingSeconds())))
        sleep(hs.delay.remainingSeconds())
        hs.checkSends()
        self.assertEqual(2, len(getattr(self, 'sends', [])))
        self.assertEqual(intents[0], self.sends[0])
        self.assertEqual(intents[-1], self.sends[1])
        self.assertEqual(20, len(getattr(self, 'successes', [])))
        self.assertEqual(0, len(getattr(self, 'fails', [])))

    def testTwentySendsSameAddressSameMessageTypeCancelCallbacks(self):
        self.sends = []
        self.fails = []
        hs = HysteresisDelaySender(self.send,
                                   hysteresis_min_period = timedelta(milliseconds=2),
                                   hysteresis_max_period = timedelta(milliseconds=10),
                                   hysteresis_rate = 2)
        intents = [TransmitIntent('addr1', 'msg1')]
        for num in range(20):
            intents.append(TransmitIntent('addr1', 'msg',
                                          onSuccess = self.successfulIntent,
                                          onError = self.failedIntent))
        for each in intents:
            hs.sendWithHysteresis(each)
        # First was sent immediately, all others are delayed
        self.assertEqual(1, len(getattr(self, 'sends', [])))
        self.assertEqual(intents[0], self.sends[0])
        self.assertNotEqual(timedelta(seconds=0), hs.delay.remaining())
        self.assertGreaterEqual(timedelta(milliseconds=10), hs.delay.remaining())
        self.assertLess(timedelta(milliseconds=9), hs.delay.remaining())
        hs.cancelSends('addr1')
        self.assertEqual(1, len(getattr(self, 'sends', [])))
        self.assertEqual(intents[0], self.sends[0])
        self.assertEqual(0, len(getattr(self, 'successes', [])))
        self.assertEqual(20, len(getattr(self, 'fails', [])))
        hs.checkSends()
        self.assertEqual(1, len(getattr(self, 'sends', [])))
        self.assertEqual(intents[0], self.sends[0])
        self.assertEqual(0, len(getattr(self, 'successes', [])))
        self.assertEqual(20, len(getattr(self, 'fails', [])))

    def testEighteenSendsSameAddressThreeDifferentMessageTypes(self):
        self.sends = []
        hs = HysteresisDelaySender(self.send,
                                   hysteresis_min_period = timedelta(milliseconds=2),
                                   hysteresis_max_period = timedelta(milliseconds=10),
                                   hysteresis_rate = 2)
        intents = [TransmitIntent('addr1', 'msg1')]
        for num in range(18):
            if num % 3 == 0: msg = 'msg'
            elif num % 3 == 1: msg = 1
            else: msg = True
            intents.append(TransmitIntent('addr1', msg))
        for each in intents:
            hs.sendWithHysteresis(each)
        # First was sent immediately, all others are delayed
        self.assertEqual(1, len(getattr(self, 'sends', [])))
        self.assertEqual(intents[0], self.sends[0])
        self.assertNotEqual(timedelta(seconds=0), hs.delay.remaining())
        self.assertGreaterEqual(timedelta(milliseconds=10), hs.delay.remaining())
        self.assertLess(timedelta(milliseconds=9), hs.delay.remaining())
        print('Remaining seconds: %s (%s)'%(hs.delay.remainingSeconds(),
                                            type(hs.delay.remainingSeconds())))
        sleep(hs.delay.remainingSeconds())
        hs.checkSends()
        # Only should have first message and last of each type
        self.assertEqual(4, len(getattr(self, 'sends', [])))
        self.assertEqual(intents[0], self.sends[0])
        self.assertEqual(intents[-3], self.sends[1])
        self.assertEqual(intents[-2], self.sends[2])
        self.assertEqual(intents[-1], self.sends[3])

    def testTwentySendsSameAddressSameMessageTypeSendAfterDelay(self):
        self.sends = []
        hs = HysteresisDelaySender(self.send,
                                   hysteresis_min_period = timedelta(milliseconds=2),
                                   hysteresis_max_period = timedelta(milliseconds=20),
                                   hysteresis_rate = 2)
        intents = [TransmitIntent('addr1', 'msg1')]
        for num in range(20):
            intents.append(TransmitIntent('addr1', 'msg'))
        for each in intents:
            hs.sendWithHysteresis(each)
        # First was sent immediately, all others are delayed
        self.assertEqual(1, len(getattr(self, 'sends', [])))
        self.assertEqual(intents[0], self.sends[0])
        # The hysteresis delay should be maxed out
        t1 = hs.delay.remaining()
        self.assertNotEqual(timedelta(seconds=0), t1)
        self.assertGreaterEqual(timedelta(milliseconds=20), t1)
        self.assertLess(timedelta(milliseconds=9), t1)
        # Wait the delay period and then check, which should send the
        # (latest) queued messages
        sleep(hs.delay.remainingSeconds())
        hs.checkSends()
        # Verify that hysteresis delay is not yet back to zero and
        # additional sends are still blocked.
        self.assertFalse(hs.delay.expired())  # got refreshed and reduced in checkSends
        hs.sendWithHysteresis(intents[0])
        self.assertEqual(2, len(getattr(self, 'sends', [])))
        self.assertEqual(intents[0], self.sends[0])
        self.assertEqual(intents[-1], self.sends[1])
        # Verify that the hysteresis delay keeps dropping and
        # eventually gets back to zero.  After a drop, any pending
        # sends that were blocked should be sent.
        nsent = 2  # after first wait, checkSends will send the one just queued...
        for x in range(100):  # don't loop forever
            if hs.delay.expired(): break
            t2 = hs.delay.remaining()
            self.assertNotEqual(timedelta(seconds=0), t2)
            self.assertLess(t2, t1)
            self.assertEqual(nsent, len(getattr(self, 'sends', [])))
            sleep(hs.delay.remainingSeconds())
            t1 = t2
            hs.checkSends()
            if nsent == 2: nsent = 3
        # Now verify hysteresis sender is back to the original state
        self.assertEqual(3, len(getattr(self, 'sends', [])))
        hs.sendWithHysteresis(intents[1])
        self.assertEqual(4, len(getattr(self, 'sends', [])))
        self.assertEqual(intents[0], self.sends[0])
        self.assertEqual(intents[-1], self.sends[1])
        self.assertEqual(intents[0], self.sends[2])
        self.assertEqual(intents[1], self.sends[3])
