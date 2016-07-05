from pytest import raises
from thespian.test import *
import time
from thespian.actors import *


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


class TestFuncCreateActor():

    def test00_systemsRunnable(self, asys):
        pass

    def test01_verifyFooActorNotAvailableByName(self, asys):
        raises(ImportError, asys.createActor, 'foo.FooActor')

    def test02_verifyBarActorAvailableByReference(self, asys):
        bar = asys.createActor(BarActor)
        assert bar is not None
        assert 'SAW: hello' == asys.ask(bar, 'hello', 1)

    def test03_simpleBarActorStringNotSupported(self, asys):
        raises(InvalidActorSpecification, asys.createActor, 'BarActor')

    def test03_verifyActorAvailableToOtherActorsInSameScopeByShortName(self, asys):
        cow = asys.createActor(CowActor)
        assert cow is not None
        assert 'MOO: you' == asys.ask(cow, 'you', 1)
        assert 'Saw: 99' == asys.ask(cow, 99, 1)

    def test03_verifyBarActorAvailableByFullyQualifiedName(self, asys):
        bar = asys.createActor('thespian.test.test_createActor.BarActor')
        assert bar is not None
        assert 'SAW: greetings' == asys.ask(bar, 'greetings', 1)

    def test04_verifyLowerFloorActorAvailableByName(self, asys):
        low = asys.createActor('thespian.test.sub1.sub2.lower.LowerFloor')
        assert low is not None
        assert 'Heard: pin drop' == asys.ask(low, 'pin drop', 1)

    def test05_verifyActorCanCreateSubActorByName(self, asys):
        upper = asys.createActor('thespian.test.sub1.upper.UpperFloor')
        assert upper is not None
        assert 'Viewed: picture show' == asys.ask(upper, 'picture show', 1)
        assert 'And Heard: soundtrack' == asys.ask(upper, (None, 'soundtrack'), 1)
