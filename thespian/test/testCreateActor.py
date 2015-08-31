import unittest
from thespian.test import ActorSystemTestCase
import time
import thespian.test.helpers
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
            bar = self.createActor('thespian.test.testCreateActor.BarActor')
            self.send(bar, (sender, str(msg)))


class TestASimpleSystem(ActorSystemTestCase):
    testbase='Simple'
    scope='func'

    def test00_systemsRunnable(self):
        pass

    def test01_verifyFooActorNotAvailableByName(self):
        self.assertRaises(ImportError, ActorSystem().createActor, 'foo.FooActor')

    def test02_verifyBarActorAvailableByReference(self):
        aS = ActorSystem()
        bar = aS.createActor(BarActor)
        self.assertIsNotNone(bar)
        self.assertEqual('SAW: hello', aS.ask(bar, 'hello', 1))

    def test03_simpleBarActorStringNotSupported(self):
        aS = ActorSystem()
        self.assertRaises(InvalidActorSpecification,
                          aS.createActor, 'BarActor')

    def test03_verifyActorAvailableToOtherActorsInSameScopeByShortName(self):
        aS = ActorSystem()
        cow = aS.createActor(CowActor)
        self.assertIsNotNone(cow)
        self.assertEqual('MOO: you', aS.ask(cow, 'you', 1))
        self.assertEqual('Saw: 99', aS.ask(cow, 99, 1))

    def test03_verifyBarActorAvailableByFullyQualifiedName(self):
        aS = ActorSystem()
        bar = aS.createActor('thespian.test.testCreateActor.BarActor')
        self.assertIsNotNone(bar)
        self.assertEqual('SAW: greetings', aS.ask(bar, 'greetings', 1))

    def test04_verifyLowerFloorActorAvailableByName(self):
        aS = ActorSystem()
        low = aS.createActor('thespian.test.sub1.sub2.lower.LowerFloor')
        self.assertIsNotNone(low)
        self.assertEqual('Heard: pin drop', aS.ask(low, 'pin drop', 1))

    def test05_verifyActorCanCreateSubActorByName(self):
        aS = ActorSystem()
        upper = aS.createActor('thespian.test.sub1.upper.UpperFloor')
        self.assertIsNotNone(upper)
        self.assertEqual('Viewed: picture show', aS.ask(upper, 'picture show', 1))
        self.assertEqual('And Heard: soundtrack', aS.ask(upper, (None, 'soundtrack'), 1))



class TestMultiprocQueueSystem(TestASimpleSystem):
    testbase='MultiprocQueue'
    scope='func'

    def setUp(self):
        self.setSystemBase('multiprocQueueBase')
        super(TestMultiprocQueueSystem, self).setUp()


class TestMultiprocUDPSystem(TestASimpleSystem):
    testbase='MultiprocUDP'
    scope='func'

    def setUp(self):
        self.setSystemBase('multiprocUDPBase')
        super(TestMultiprocUDPSystem, self).setUp()


class TestMultiprocTCPSystem(TestASimpleSystem):
    testbase='MultiprocTCP'
    scope='func'

    def setUp(self):
        self.setSystemBase('multiprocTCPBase')
        super(TestMultiprocTCPSystem, self).setUp()
