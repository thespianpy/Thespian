import unittest
import logging
import time, datetime
import thespian.test.helpers
from thespian.actors import *
from thespian.test import ActorSystemTestCase

class Larry(Actor):
    def receiveMessage(self, msg, sender):
        if msg != 'Silence!':
            self.send(sender, 'Hey!')


class Mo(Actor):
    def receiveMessage(self, msg, sender):
        pass

class Curly(Actor):
    def receiveMessage(self, msg, sender):
        logging.debug('Sending msg1')
        self.send(sender, 'Wise guy, eh?')
        logging.debug('Sending msg2')
        self.send(sender, 'Pow!')


class TestASimpleSystem(ActorSystemTestCase):
    testbase='Simple'
    scope='func'

    responseDelay = 0.06

    def testTell(self):
        mo = ActorSystem().createActor(Mo)
        ActorSystem().tell(mo, 'hello')
        ActorSystem().tell(mo, 'goodbye')

    def testAsk(self):
        larry = ActorSystem().createActor(Larry)
        rsp = ActorSystem().ask(larry, 'hello', self.responseDelay)
        self.assertEqual(rsp, 'Hey!')
        rsp = ActorSystem().ask(larry, 'Silence!', self.responseDelay)
        self.assertIsNone(rsp)

    def testListen(self):
        curly = ActorSystem().createActor(Curly)
        rsp = ActorSystem().ask(curly, 'hello', self.responseDelay)
        self.assertEqual(rsp, 'Wise guy, eh?')
        rsp = ActorSystem().listen(self.responseDelay)
        self.assertEqual(rsp, 'Pow!')

    def testAskIsTellPlusListen(self):
        larry = ActorSystem().createActor(Larry)
        rsp = ActorSystem().ask(larry, 'hello', self.responseDelay)
        self.assertEqual(rsp, 'Hey!')

        rsp = ActorSystem().listen(self.responseDelay)
        self.assertIsNone(rsp)

        ActorSystem().tell(larry, 'hello')
        rsp = ActorSystem().listen(self.responseDelay)
        self.assertEqual(rsp, 'Hey!')

        rsp = ActorSystem().listen(self.responseDelay)
        self.assertIsNone(rsp)

    def testResponsesFromAnywhere(self):
        aS = ActorSystem()
        larry = aS.createActor(Larry)
        mo    = aS.createActor(Mo)
        curly = aS.createActor(Curly)

        aS.tell(curly, 'hello')
        aS.tell(mo, 'hello')
        rsp = aS.ask(larry, 'hello', self.responseDelay)

        responses = [ 'Wise guy, eh?', 'Pow!', 'Hey!' ]
        while responses:
            self.assertIsNotNone(rsp)
            self.assertIn(rsp, responses)
            responses = [R for R in responses if R != rsp]
            rsp = ActorSystem().listen(self.responseDelay)


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

