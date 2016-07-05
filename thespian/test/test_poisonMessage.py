import time
from thespian.test import *
from thespian.actors import *


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
            cnt = asys.ask(counter, "Count?", 1)
            if newCount == cnt:
                break
            time.sleep(0.01)
        assert asys.ask(counter, "Count?", 1) == newCount

    def testBadNews(self, asys):
        counter = asys.createActor(PoisonCounter)
        t1 = asys.createActor(TestActor)
        asys.tell(t1, counter)
        assert asys.ask(counter, "Count?", 1) == 0
        asys.tell(t1, 1)
        self.waitForCount(asys, counter, 1)
        asys.tell(t1, ActorExitRequest())


    def testLevel2(self, asys):
        counter = asys.createActor(PoisonCounter)
        t1 = asys.createActor(TestActor)
        asys.tell(t1, counter)
        assert asys.ask(counter, "Count?", 1) == 0
        asys.tell(t1, 2)
        self.waitForCount(asys, counter, 1)
        asys.tell(t1, 2)
        asys.tell(t1, 2)
        self.waitForCount(asys, counter, 3)


    def testLevel1(self, asys):
        counter = asys.createActor(PoisonCounter)
        t1 = asys.createActor(TestActor)
        asys.tell(t1, counter)
        assert asys.ask(counter, "Count?", 1) == 0
        asys.tell(t1, 1)
        self.waitForCount(asys, counter, 1)
        asys.tell(t1, 1)
        asys.tell(t1, 1)
        self.waitForCount(asys, counter, 3)

    def testLevel3tell(self, asys):
        counter = asys.createActor(PoisonCounter)
        t1 = asys.createActor(TestActor)
        asys.tell(t1, counter)
        assert asys.ask(counter, "Count?", 1) == 0
        assert asys.ask(t1, "Hello", 1) == 'hi'
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

        assert asys.ask(counter, "Count?", 1) == 0
        assert asys.ask(t1, "Hello", 1) == 'hi'
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

        assert asys.ask(counter, "Count?", 1) == 0
        assert asys.ask(t1, "Hello", 1) == 'hi'


    def testLevel3ask(self, asys):
        counter = asys.createActor(PoisonCounter)
        t1 = asys.createActor(TestActor)
        asys.tell(t1, counter)
        assert asys.ask(counter, "Count?", 1) == 0
        assert asys.ask(t1, "Hello", 1) == 'hi'
        # Send a message that will cause the TestActor to throw an
        # exception.  The system will restart the TestActor and
        # re-attempt delivery, but on multiple failures it will send
        # back the request to the originator in a PoisonMessage
        # wrapper.
        rsp = asys.ask(t1, 3, 1)
        assert isinstance(rsp, PoisonMessage)
        assert rsp.poisonMessage == 3

        assert asys.ask(counter, "Count?", 1) == 0
        assert asys.ask(t1, "Hello", 1) == 'hi'
        asys.ask(t1, 3, 1)
        rsp = asys.ask(t1, 3, 1)
        assert isinstance(rsp, PoisonMessage)
        assert rsp.poisonMessage == 3
        assert asys.ask(counter, "Count?", 1) == 0


    def testUseBadAddressInActorGetsReturnedAsPoison(self, asys):
        dummy = asys.createActor(Dummy)
        resp = asys.ask(dummy, 'Aside', 0.5)
        assert isinstance(resp, PoisonMessage)
        assert resp.poisonMessage == 'Aside'
        assert asys.ask(dummy, 'hello', 0.5) == 'Greetings.'
