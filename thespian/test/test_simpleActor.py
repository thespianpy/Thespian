import logging
import time, datetime
from thespian.test import *
from thespian.actors import *


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


class TestFuncSimpleActorOperations(object):
    def testCreateActorSystem(self, asys):
        pass

    def testSimpleActor(self, asys):
        clooney = asys.createActor(Clooney)

    def testSimpleActorTell(self, asys):
        clooney = asys.createActor(Clooney)
        asys.tell(clooney, 'hello')
        time.sleep(0.02)  # allow tell to work before ActorSystem shutdown

    def testSimpleActorTellAbort(self, asys):
        clooney = asys.createActor(Clooney)
        asys.tell(clooney, 'hello')
        # no waiting: attempt system shutdown immediately which may or
        # may not occur before clooney is fully greeted.

    def testSimpleActorAsk(self, asys):
        clooney = asys.createActor(Clooney)
        r = asys.ask(clooney, 'hello', 3.5)
        assert r == 'Greetings.'

    def testSimpleActorAskTimeout(self, asys):
        clooney = asys.createActor(Clooney)
        t1 = datetime.datetime.now()
        r = asys.ask(clooney, 'Silence!', 0.5)
        assert r == None
        # Could test that it waited the proper amount of time, but
        # that doesn't allow an ActorSystems that knows there will be
        # no response to run more quickly (e.g. simpleActorSystem).
        #
        # t2 = datetime.datetime.now()
        # self.assertGreaterEqual(t2 - t1, datetime.timedelta(microseconds=500*1000))

    def testSendTupleToSelf(self, asys):
        hamlet = asys.createActor(Hamlet)
        r = asys.ask(hamlet, 'Alas, poor Yorick!', 3)
        assert r == 'That is the question.'
