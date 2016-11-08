import time
from datetime import timedelta
from thespian.test import *
from thespian.actors import *


ask_wait = timedelta(seconds=8)
count_update_wait = lambda: inTestDelay(timedelta(milliseconds=100))


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


class TestFuncPoisonMessage(object):

    def waitForCount(self, asys, counter, newCount):
        for X in range(50):
            cnt = asys.ask(counter, "Count?", ask_wait)
            if newCount == cnt:
                break
            count_update_wait()
        r = asys.ask(counter, "Count?", ask_wait)
        assert r == newCount

    def testBadNews(self, asys):
        counter = asys.createActor(PoisonCounter)
        t1 = asys.createActor(TestActor)
        asys.tell(t1, counter)
        r = asys.ask(counter, "Count?", ask_wait)
        assert r == 0
        asys.tell(t1, 1)
        self.waitForCount(asys, counter, 1)
        asys.tell(t1, ActorExitRequest())


    def testLevel2(self, asys):
        counter = asys.createActor(PoisonCounter)
        t1 = asys.createActor(TestActor)
        asys.tell(t1, counter)
        r = asys.ask(counter, "Count?", ask_wait)
        assert r == 0
        asys.tell(t1, 2)
        self.waitForCount(asys, counter, 1)
        asys.tell(t1, 2)
        asys.tell(t1, 2)
        self.waitForCount(asys, counter, 3)


    def testLevel1(self, asys):
        counter = asys.createActor(PoisonCounter)
        t1 = asys.createActor(TestActor)
        asys.tell(t1, counter)
        r = asys.ask(counter, "Count?", ask_wait)
        assert r == 0
        asys.tell(t1, 1)
        self.waitForCount(asys, counter, 1)
        asys.tell(t1, 1)
        asys.tell(t1, 1)
        self.waitForCount(asys, counter, 3)

    def testLevel3tell(self, asys):
        counter = asys.createActor(PoisonCounter)
        t1 = asys.createActor(TestActor)
        asys.tell(t1, counter)
        r = asys.ask(counter, "Count?", ask_wait)
        assert r == 0
        r = asys.ask(t1, "Hello", ask_wait)
        assert r == 'hi'
        # Send a message that will cause the TestActor to throw an
        # exception.  The system will restart the TestActor and
        # re-attempt delivery, but on multiple failures it will send
        # back the request to the originator in a PoisonMessage
        # wrapper.
        asys.tell(t1, 3)
        time.sleep(0.01)
        # Flush out any PoisonMessage response
        r = asys.ask(t1, 'nothing', 0.2)
        if r:
            assert isinstance(r, PoisonMessage)
            assert r.poisonMessage == 3
            assert 'Yucky number 3' in r.details

        r = asys.ask(counter, "Count?", ask_wait)
        assert r == 0
        r = asys.ask(t1, "Hello", ask_wait)
        assert r == 'hi'
        asys.tell(t1, 3)
        asys.tell(t1, 3)
        time.sleep(0.01)
        # Flush out any PoisonMessage responses
        r = asys.ask(t1, 'nothing', 0.2)
        if r:
            assert isinstance(r, PoisonMessage)
            assert r.poisonMessage == 3
        r = asys.ask(t1, 'nothing', 0.2)
        if r:
            assert isinstance(r, PoisonMessage)
            assert r.poisonMessage == 3

        r = asys.ask(counter, "Count?", ask_wait)
        assert r == 0
        r =  asys.ask(t1, "Hello", ask_wait)
        assert r == 'hi'


    def testLevel3ask(self, asys):
        counter = asys.createActor(PoisonCounter)
        t1 = asys.createActor(TestActor)
        asys.tell(t1, counter)
        r = asys.ask(counter, "Count?", ask_wait)
        assert r == 0
        r = asys.ask(t1, "Hello", ask_wait)
        assert r == 'hi'
        # Send a message that will cause the TestActor to throw an
        # exception.  The system will restart the TestActor and
        # re-attempt delivery, but on multiple failures it will send
        # back the request to the originator in a PoisonMessage
        # wrapper.
        rsp = asys.ask(t1, 3, ask_wait)
        assert isinstance(rsp, PoisonMessage)
        assert rsp.poisonMessage == 3

        r = asys.ask(counter, "Count?", ask_wait)
        assert r == 0
        r = asys.ask(t1, "Hello", ask_wait)
        assert r == 'hi'
        asys.ask(t1, 3, ask_wait)
        rsp = asys.ask(t1, 3, ask_wait)
        assert isinstance(rsp, PoisonMessage)
        assert rsp.poisonMessage == 3
        r = asys.ask(counter, "Count?", ask_wait)
        assert r == 0


    def testUseBadAddressInActorGetsReturnedAsPoison(self, asys):
        dummy = asys.createActor(Dummy)
        resp = asys.ask(dummy, 'Aside', ask_wait)
        assert isinstance(resp, PoisonMessage)
        assert resp.poisonMessage == 'Aside'
        assert 'InvalidActorAddress' in resp.details
        r = asys.ask(dummy, 'hello', ask_wait)
        assert r == 'Greetings.'
