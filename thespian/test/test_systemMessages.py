from thespian.test import *
import logging
import time, datetime
from thespian.actors import *


class EchoActor(Actor):
    def receiveMessage(self, msg, sender):
        logging.info('EchoActor got %s (%s) from %s', msg, type(msg), sender)
        self.send(sender, msg)


class Kill_The_Messenger(Actor):
    def receiveMessage(self, message, sender):
        self.send(sender, ActorExitRequest())


class FakeSystemMessage(ActorSystemMessage):
    pass


smallwait = datetime.timedelta(milliseconds=200)


class TestFuncSystemMessages(object):

    def testCreateActorSystem(self, asys):
        pass

    def testSimpleActor(self, asys):
        echo = asys.createActor(EchoActor)

    def testSimpleMessageTell(self, asys):
        echo = asys.createActor(EchoActor)
        asys.tell(echo, 'hello')
        time.sleep(0.02)  # allow tell to work before ActorSystem shutdown

    def testSystemMessageTell(self, asys):
        echo = asys.createActor(EchoActor)
        asys.tell(echo, FakeSystemMessage())
        time.sleep(0.02)  # allow tell to work before ActorSystem shutdown

    def testKillMessageTell(self, asys):
        echo = asys.createActor(EchoActor)
        asys.tell(echo, ActorExitRequest())
        time.sleep(0.02)  # allow tell to work before ActorSystem shutdown

    def testKillMessageTellKiller(self, asys):
        ktm = asys.createActor(Kill_The_Messenger)
        asys.tell(ktm, 'hello')
        asys.tell(ktm, ActorExitRequest())
        time.sleep(0.02)  # allow tell to work before ActorSystem shutdown

    def testSimpleMessageAsk(self, asys):
        echo = asys.createActor(EchoActor)
        assert asys.ask(echo, 'hello', smallwait) == 'hello'

    def testSystemMessageAsk(self, asys):
        echo = asys.createActor(EchoActor)
        # SystemMessages are explicitly filtered from being returned
        # via Ask() or Tell(), with the exception of PoisonMessage.
        assert asys.ask(echo, FakeSystemMessage(), smallwait) is None

    def testKillMessageAsk(self, asys):
        echo = asys.createActor(EchoActor)
        # SystemMessages are explicitly filtered from being returned
        # via Ask() or Tell(), with the exception of PoisonMessage.
        assert asys.ask(echo, ActorExitRequest(), smallwait) is None

    def testKillMessageAskKiller(self, asys):
        ktm = asys.createActor(Kill_The_Messenger)
        assert asys.ask(ktm, 'hello', smallwait) is None
        assert asys.ask(ktm, ActorExitRequest(), smallwait) is None
