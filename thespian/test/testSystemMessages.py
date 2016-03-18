import unittest
import logging
import time, datetime
import thespian.test.helpers
from thespian.actors import *
from thespian.test import ActorSystemTestCase


class EchoActor(Actor):
    def receiveMessage(self, msg, sender):
        logging.info('EchoActor got %s (%s) from %s', msg, type(msg), sender)
        self.send(sender, msg)


class Kill_The_Messenger(Actor):
    def receiveMessage(self, message, sender):
        self.send(sender, ActorExitRequest())


class FakeSystemMessage(ActorSystemMessage):
    pass


smallwait = datetime.timedelta(milliseconds=50)


class TestASimpleSystem(ActorSystemTestCase):
    testbase='Simple'
    scope='func'

    def testCreateActorSystem(self):
        pass

    def testSimpleActor(self):
        echo = ActorSystem().createActor(EchoActor)

    def testSimpleMessageTell(self):
        echo = ActorSystem().createActor(EchoActor)
        ActorSystem().tell(echo, 'hello')
        time.sleep(0.02)  # allow tell to work before ActorSystem shutdown

    def testSystemMessageTell(self):
        echo = ActorSystem().createActor(EchoActor)
        ActorSystem().tell(echo, FakeSystemMessage())
        time.sleep(0.02)  # allow tell to work before ActorSystem shutdown

    def testKillMessageTell(self):
        echo = ActorSystem().createActor(EchoActor)
        ActorSystem().tell(echo, ActorExitRequest())
        time.sleep(0.02)  # allow tell to work before ActorSystem shutdown

    def testKillMessageTellKiller(self):
        ktm = ActorSystem().createActor(Kill_The_Messenger)
        ActorSystem().tell(ktm, 'hello')
        ActorSystem().tell(ktm, ActorExitRequest())
        time.sleep(0.02)  # allow tell to work before ActorSystem shutdown

    def testSimpleMessageAsk(self):
        echo = ActorSystem().createActor(EchoActor)
        self.assertEqual(ActorSystem().ask(echo, 'hello', smallwait), 'hello')

    def testSystemMessageAsk(self):
        echo = ActorSystem().createActor(EchoActor)
        # SystemMessages are explicitly filtered from being returned
        # via Ask() or Tell(), with the exception of PoisonMessage.
        self.assertIsNone(ActorSystem().ask(echo, FakeSystemMessage(), smallwait))

    def testKillMessageAsk(self):
        echo = ActorSystem().createActor(EchoActor)
        # SystemMessages are explicitly filtered from being returned
        # via Ask() or Tell(), with the exception of PoisonMessage.
        self.assertIsNone(ActorSystem().ask(echo, ActorExitRequest(), smallwait))

    def testKillMessageAskKiller(self):
        ktm = ActorSystem().createActor(Kill_The_Messenger)
        self.assertIsNone(ActorSystem().ask(ktm, 'hello', smallwait))
        self.assertIsNone(ActorSystem().ask(ktm, ActorExitRequest(), smallwait))


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

