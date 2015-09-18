import unittest
import time
import thespian.test.helpers
from thespian.actors import *
from thespian.test import ActorSystemTestCase


class PoisonCounter(Actor):
    def __init__(self, *args, **kw):
        super(PoisonCounter, self).__init__(*args, **kw)
        self.num_poisoned = 0
        self.failed = None
    def receiveMessage(self, msg, sender):
        if "Count?" == msg:
            if self.failed:
                self.send(sender, self.failed)
            else:
                self.send(sender, self.num_poisoned)
        elif "Poisoned" == msg:
            self.num_poisoned += 1
        elif type(msg) == type((1,2)) and "PoisonMostFoul" == msg[0]:
            self.failed = msg[1]


class TestActor(Actor):
    def receiveMessage(self, msg, sender):
        if isinstance(msg, PoisonMessage):
            if sender == self.myAddress:
                self.send(self.recorder, ("PoisonMostFoul", "Got PoisonMessage from self!"))
            else:
                self.send(self.recorder, "Poisoned")
        elif isinstance(msg, ActorAddress):
            self.recorder = msg
        elif isinstance(msg, int):
            if msg == 3:
                raise ValueError('Yucky number 3')
            next = self.createActor(TestActor)
            self.send(next, self.recorder)
            self.send(next, msg+1)
        elif "Hello" == msg:
            self.send(sender, "hi")
        else:
            pass


class NoBadNewsActor(TestActor):
    def receiveMessage(self, msg, sender):
        if isinstance(msg, ChildActorExited):
            raise IndexError('Do not give me bad news')
        super(NoBadNewsActor, self).receiveMessage(msg, sender)


class Dummy(Actor):
    def receiveMessage(self, msg, sender):
        if msg == 'Silence!':
            pass
        elif msg == 'Aside':
            self.send('Audience', 'hello')
        else:
            self.send(sender, 'Greetings.')


class TestASimpleSystem(ActorSystemTestCase):
    testbase='Simple'
    scope='func'

    def waitForCount(self, counter, newCount):
        aS = ActorSystem()
        for X in range(50):
            cnt = aS.ask(counter, "Count?")
            if newCount == cnt:
                break
            time.sleep(0.01)
        self.assertEqual(aS.ask(counter, "Count?"), newCount)

    def testBadNews(self):
        aS = ActorSystem()
        counter = aS.createActor(PoisonCounter)
        t1 = aS.createActor(TestActor)
        aS.tell(t1, counter)
        self.assertEqual(aS.ask(counter, "Count?"), 0)
        aS.tell(t1, 1)
        self.waitForCount(counter, 1)
        aS.tell(t1, ActorExitRequest())


    def testLevel2(self):
        aS = ActorSystem()
        counter = aS.createActor(PoisonCounter)
        t1 = aS.createActor(TestActor)
        aS.tell(t1, counter)
        self.assertEqual(aS.ask(counter, "Count?"), 0)
        aS.tell(t1, 2)
        self.waitForCount(counter, 1)
        aS.tell(t1, 2)
        aS.tell(t1, 2)
        self.waitForCount(counter, 3)


    def testLevel1(self):
        aS = ActorSystem()
        counter = aS.createActor(PoisonCounter)
        t1 = aS.createActor(TestActor)
        aS.tell(t1, counter)
        self.assertEqual(aS.ask(counter, "Count?"), 0)
        aS.tell(t1, 1)
        self.waitForCount(counter, 1)
        aS.tell(t1, 1)
        aS.tell(t1, 1)
        self.waitForCount(counter, 3)

    def testLevel3tell(self):
        aS = ActorSystem()
        counter = aS.createActor(PoisonCounter)
        t1 = aS.createActor(TestActor)
        aS.tell(t1, counter)
        self.assertEqual(aS.ask(counter, "Count?"), 0)
        self.assertEqual(aS.ask(t1, "Hello"), 'hi')
        # Send a message that will cause the TestActor to throw an
        # exception.  The system will restart the TestActor and
        # re-attempt delivery, but on multiple failures it will send
        # back the request to the originator in a PoisonMessage
        # wrapper.
        aS.tell(t1, 3)
        time.sleep(0.01)
        # Flush out any PoisonMessage response
        r = aS.ask(t1, 'nothing', 0.2)
        if r:
            self.assertIsInstance(r, PoisonMessage)
            self.assertEqual(r.poisonMessage, 3)

        self.assertEqual(aS.ask(counter, "Count?"), 0)
        self.assertEqual(aS.ask(t1, "Hello"), 'hi')
        aS.tell(t1, 3)
        aS.tell(t1, 3)
        time.sleep(0.01)
        # Flush out any PoisonMessage responses
        r = aS.ask(t1, 'nothing', 0.2)
        if r:
            self.assertIsInstance(r, PoisonMessage)
            self.assertEqual(r.poisonMessage, 3)
        r = aS.ask(t1, 'nothing', 0.2)
        if r:
            self.assertIsInstance(r, PoisonMessage)
            self.assertEqual(r.poisonMessage, 3)

        self.assertEqual(aS.ask(counter, "Count?"), 0)
        self.assertEqual(aS.ask(t1, "Hello"), 'hi')


    def testLevel3ask(self):
        aS = ActorSystem()
        counter = aS.createActor(PoisonCounter)
        t1 = aS.createActor(TestActor)
        aS.tell(t1, counter)
        self.assertEqual(aS.ask(counter, "Count?"), 0)
        self.assertEqual(aS.ask(t1, "Hello"), 'hi')
        # Send a message that will cause the TestActor to throw an
        # exception.  The system will restart the TestActor and
        # re-attempt delivery, but on multiple failures it will send
        # back the request to the originator in a PoisonMessage
        # wrapper.
        rsp = aS.ask(t1, 3)
        self.assertIsInstance(rsp, PoisonMessage)
        self.assertEqual(rsp.poisonMessage, 3)

        self.assertEqual(aS.ask(counter, "Count?"), 0)
        self.assertEqual(aS.ask(t1, "Hello"), 'hi')
        aS.ask(t1, 3)
        rsp = aS.ask(t1, 3)
        self.assertIsInstance(rsp, PoisonMessage)
        self.assertEqual(rsp.poisonMessage, 3)
        self.assertEqual(aS.ask(counter, "Count?"), 0)


    def testUseBadAddressInActorGetsReturnedAsPoison(self):
        dummy = ActorSystem().createActor(Dummy)
        resp = ActorSystem().ask(dummy, 'Aside', 0.5)
        self.assertIsInstance(resp, PoisonMessage)
        self.assertEqual(resp.poisonMessage, 'Aside')
        self.assertEqual(ActorSystem().ask(dummy, 'hello', 0.5), 'Greetings.')


class TestMultiprocUDPSystem(TestASimpleSystem):
    testbase='MultiprocUDP'
    def setUp(self):
        self.setSystemBase('multiprocUDPBase')
        super(TestMultiprocUDPSystem, self).setUp()

class TestMultiprocTCPSystem(TestASimpleSystem):
    testbase='MultiprocTCP'
    def setUp(self):
        self.setSystemBase('multiprocTCPBase')
        super(TestMultiprocTCPSystem, self).setUp()

class TestMultiprocQueueSystem(TestASimpleSystem):
    testbase='MultiprocQueue'
    def setUp(self):
        self.setSystemBase('multiprocQueueBase')
        super(TestMultiprocQueueSystem, self).setUp()
