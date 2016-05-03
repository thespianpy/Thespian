import unittest
import logging
import time, datetime
import thespian.test.helpers
from thespian.actors import *
from thespian.test import TestSystem


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


class TestASimpleSystem(unittest.TestCase):
    testbase='Simple'
    scope='func'

    sysbase = 'simpleSystemBase'
    portbase = 17100

    def testCreateActorSystem(self):
        with TestSystem(self.sysbase, {'Admin Port': self.portbase+1}) as asys:
            pass

    def testSimpleActor(self):
        with TestSystem(self.sysbase, {'Admin Port': self.portbase+2}) as asys:
            echo = asys.createActor(EchoActor)

    def testSimpleMessageTell(self):
        with TestSystem(self.sysbase, {'Admin Port': self.portbase+3}) as asys:
            echo = asys.createActor(EchoActor)
            asys.tell(echo, 'hello')
            time.sleep(0.02)  # allow tell to work before ActorSystem shutdown

    def testSystemMessageTell(self):
        with TestSystem(self.sysbase, {'Admin Port': self.portbase+4}) as asys:
            echo = asys.createActor(EchoActor)
            asys.tell(echo, FakeSystemMessage())
            time.sleep(0.02)  # allow tell to work before ActorSystem shutdown

    def testKillMessageTell(self):
        with TestSystem(self.sysbase, {'Admin Port': self.portbase+5}) as asys:
            echo = asys.createActor(EchoActor)
            asys.tell(echo, ActorExitRequest())
            time.sleep(0.02)  # allow tell to work before ActorSystem shutdown

    def testKillMessageTellKiller(self):
        with TestSystem(self.sysbase, {'Admin Port': self.portbase+6}) as asys:
            ktm = asys.createActor(Kill_The_Messenger)
            asys.tell(ktm, 'hello')
            asys.tell(ktm, ActorExitRequest())
            time.sleep(0.02)  # allow tell to work before ActorSystem shutdown

    def testSimpleMessageAsk(self):
        with TestSystem(self.sysbase, {'Admin Port': self.portbase+7}) as asys:
            echo = asys.createActor(EchoActor)
            self.assertEqual(asys.ask(echo, 'hello', smallwait), 'hello')

    def testSystemMessageAsk(self):
        with TestSystem(self.sysbase, {'Admin Port': self.portbase+8}) as asys:
            echo = asys.createActor(EchoActor)
            # SystemMessages are explicitly filtered from being returned
            # via Ask() or Tell(), with the exception of PoisonMessage.
            self.assertIsNone(asys.ask(echo, FakeSystemMessage(), smallwait))

    def testKillMessageAsk(self):
        with TestSystem(self.sysbase, {'Admin Port': self.portbase+9}) as asys:
            echo = asys.createActor(EchoActor)
            # SystemMessages are explicitly filtered from being returned
            # via Ask() or Tell(), with the exception of PoisonMessage.
            self.assertIsNone(asys.ask(echo, ActorExitRequest(), smallwait))

    def testKillMessageAskKiller(self):
        with TestSystem(self.sysbase, {'Admin Port': self.portbase+10}) as asys:
            ktm = asys.createActor(Kill_The_Messenger)
            self.assertIsNone(asys.ask(ktm, 'hello', smallwait))
            self.assertIsNone(asys.ask(ktm, ActorExitRequest(), smallwait))


class TestMultiprocUDPSystem(TestASimpleSystem):
    testbase='MultiprocUDP'
    sysbase = 'multiprocUDPBase'
    portbase = 17120

class TestMultiprocTCPSystem(TestASimpleSystem):
    testbase='MultiprocTCP'
    sysbase = 'multiprocTCPBase'
    portbase = 17140

class TestMultiprocQueueSystem(TestASimpleSystem):
    testbase='MultiprocQueue'
    sysbase = 'multiprocQueueBase'
    portbase = 17160

