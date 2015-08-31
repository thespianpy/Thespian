import unittest
import logging
import time, datetime
import thespian.test.helpers
from thespian.actors import *
from thespian.test import ActorSystemTestCase

class Clooney(Actor):
    def receiveMessage(self, msg, sender):
        logger = logging.getLogger('Thespian.Actor')
        logger.setLevel(logging.DEBUG)
        logger.info('Clooney got message "%s" from %s'%(str(msg), str(sender)))
        if msg != 'Silence!':
            self.send(sender, 'Greetings.')


class Hamlet(Actor):
    def receiveMessage(self, msg, sender):
        if msg == 'Alas, poor Yorick!':
            self.send(self.myAddress, (sender, 'To be or not to be?'))
        if isinstance(msg, tuple) and msg[1] == 'To be or not to be?':
            self.send(msg[0], 'That is the question.')


class TestASimpleSystem(ActorSystemTestCase):
    testbase='Simple'
    scope='func'

    def testCreateActorSystem(self):
        pass

    def testSimpleActor(self):
        clooney = ActorSystem().createActor(Clooney)

    def testSimpleActorTell(self):
        clooney = ActorSystem().createActor(Clooney)
        ActorSystem().tell(clooney, 'hello')
        time.sleep(0.02)  # allow tell to work before ActorSystem shutdown

    def testSimpleActorTellAbort(self):
        clooney = ActorSystem().createActor(Clooney)
        ActorSystem().tell(clooney, 'hello')
        # no waiting: attempt system shutdown immediately which may or
        # may not occur before clooney is fully greeted.

    def testSimpleActorAsk(self):
        clooney = ActorSystem().createActor(Clooney)
        self.assertEqual(ActorSystem().ask(clooney, 'hello', 0.5), 'Greetings.')

    def testSimpleActorAskTimeout(self):
        clooney = ActorSystem().createActor(Clooney)
        t1 = datetime.datetime.now()
        self.assertEqual(ActorSystem().ask(clooney, 'Silence!', 0.5), None)
        t2 = datetime.datetime.now()
        # Could test that it waited the proper amount of time, but
        # that doesn't allow an ActorSystems that knows there will be
        # no response to run more quickly (e.g. simpleActorSystem).
        # self.assertGreaterEqual(t2 - t1, datetime.timedelta(microseconds=500*1000))

    def testSendTupleToSelf(self):
        hamlet = ActorSystem().createActor(Hamlet)
        self.assertEqual(ActorSystem().ask(hamlet, 'Alas, poor Yorick!', 1),
                         'That is the question.')


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

