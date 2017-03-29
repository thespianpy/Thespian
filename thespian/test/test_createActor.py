from pytest import raises
from thespian.test import *
from datetime import timedelta
from thespian.actors import *

max_ask_wait = timedelta(seconds=2.5)

class BarActor(Actor):
    def receiveMessage(self, msg, sender):
        if type(msg) == type(""):
            self.send(sender, 'SAW: '+str(msg))
        elif type(msg) == type( (1,2) ):
            self.send(msg[0], 'Saw: '+str(msg[1]))

class CowActor(Actor):
    def receiveMessage(self, msg, sender):
        if type(msg) == type(""):
            self.send(sender, "MOO: "+str(msg))
        elif type(msg) == type(1):
            bar = self.createActor('thespian.test.test_createActor.BarActor')
            self.send(bar, (sender, str(msg)))
            self.send(bar, ActorExitRequest())
        elif type(msg) == type([1,2]):
            barn = getattr(self, 'TheBarn', None)
            if not barn:
                barn = self.TheBarn = self.createActor(BarnActor)
            for each in msg:
                self.send(barn, ('Get A Cow', (sender, each)))
        elif isinstance(msg, tuple) and isinstance(msg[0], ActorAddress):
            self.send(msg[0], msg[1])

class BarnActor(Actor):
    def receiveMessage(self, msg, sender):
        if isinstance(msg, tuple) and msg[0] == 'Get A Cow':
            self.send(sender, (self.createActor(BarActor), msg[1]))


class TestFuncCreateActor():

    def test00_systemsRunnable(self, asys):
        pass

    def test01_verifyFooActorNotAvailableByName(self, asys):
        raises(ImportError, asys.createActor, 'foo.FooActor')

    def test02_verifyBarActorAvailableByReference(self, asys):
        bar = asys.createActor(BarActor)
        assert bar is not None
        assert 'SAW: hello' == asys.ask(bar, 'hello', max_ask_wait)

    def test03_simpleBarActorStringNotSupported(self, asys):
        raises(InvalidActorSpecification, asys.createActor, 'BarActor')

    def test03_verifyActorAvailableToOtherActorsInSameScopeByShortName(self, asys):
        cow = asys.createActor(CowActor)
        assert cow is not None
        assert 'MOO: you' == asys.ask(cow, 'you', max_ask_wait)
        assert 'Saw: 99' == asys.ask(cow, 99, max_ask_wait)

    def test03_verifyBarActorAvailableByFullyQualifiedName(self, asys):
        bar = asys.createActor('thespian.test.test_createActor.BarActor')
        assert bar is not None
        assert 'SAW: greetings' == asys.ask(bar, 'greetings', max_ask_wait)

    def test04_verifyLowerFloorActorAvailableByName(self, asys):
        low = asys.createActor('thespian.test.sub1.sub2.lower.LowerFloor')
        assert low is not None
        assert 'Heard: pin drop' == asys.ask(low, 'pin drop', max_ask_wait)

    def test05_verifyActorCanCreateSubActorByName(self, asys):
        upper = asys.createActor('thespian.test.sub1.upper.UpperFloor')
        assert upper is not None
        assert 'Viewed: picture show' == asys.ask(upper, 'picture show', max_ask_wait)
        assert 'And Heard: soundtrack' == asys.ask(upper, (None, 'soundtrack'), max_ask_wait)

    def test06_created_by_other_actor(self, asys):
        cow = asys.createActor(CowActor)
        max_count = 4
        asys.tell(cow, list(range(max_count)))
        responses = []
        for each in range(max_count):
            responses.append(asys.listen(max_ask_wait))
        for each in range(max_count):
            assert 'Saw: %d' % each in responses


class TestFuncCreateActor_LoadTorture():

    def max_count(self, asys):
        # The multiprocess Queue objects have a tendency to deadlock
        # if stressed.  If this test fails due to exceeding system
        # resource limits (e.g. process counts, file descriptor
        # counts, etc.), try reducing the max_count.
        return 10 if asys.base_name == 'multiprocQueueBase' else 100

    def test10_verify_multiple_sub_actor_creates(self, asys):
        cow = asys.createActor(CowActor)
        answers = []
        max_count = self.max_count(asys)
        for count in range(max_count):
            asys.tell(cow, count)
        for count in range(max_count):
            answers.append(asys.listen(max_ask_wait * 3))
        assert len(answers) == max_count
        for count in range(max_count):
            assert "Saw: %d" % count in answers

    def test11_created_lots_by_other_actor(self, asys):
        cow = asys.createActor(CowActor)
        max_count = self.max_count(asys)
        asys.tell(cow, list(range(max_count)))
        responses = []
        for each in range(max_count):
            responses.append(asys.listen(max_ask_wait))
        for each in range(max_count):
            assert 'Saw: %d' % each in responses
