import unittest
import random
import thespian.test.helpers
from thespian.actors import *
from thespian.test import ActorSystemTestCase

class RoutedMsg:
    def __init__(self, routes):
        self.originator = None    # set to sender if None
        self.routes     = routes  # array of ActorAddress
        self.response   = []      # array of (ActorAddress, msg)


class RoutingActor:
    def routeMessage(self, msg, sender):
        if isinstance(msg, RoutedMsg):
            if not msg.originator:
                msg.originator = sender
            if msg.routes:
                nxt = msg.routes.pop()
                msg.response.append( (self.myAddress,
                                      'passed along with %d left'%len(msg.routes)) )
                self.send(nxt, msg)
            else:
                msg.response.append( (self.myAddress, 'Ended chain') )
                self.send(msg.originator, msg)
        

class Target(Actor, RoutingActor):
    def receiveMessage(self, msg, sender):
        if type(msg) == type(''):
            self.send(sender, 'TGT %s got %s'%(self.myAddress, msg))
        elif isinstance(msg, RoutedMsg):
            self.routeMessage(msg, sender)


class Generator(Actor, RoutingActor):
    def __init__(self, *args, **kw):
        super(Generator, self).__init__(*args, **kw)
        self.generated = []
    def receiveMessage(self, msg, sender):
        if msg == 'Generate':
            self.generated.append(self.createActor(Target))
            self.send(sender, self.generated[-1])
        elif type(msg) == type(''):
            if sender in self.generated:
                orig = getattr(self, 'topsender', None)
                if orig:
                    self.send(orig, msg)
            else:
                if self.generated:
                    self.topsender = sender
                    tgt = random.choice(self.generated)
                    self.send(tgt, msg)
                else:
                    self.send(sender, 'GEN %s got %s'%(self.myAddress, msg))
        elif isinstance(msg, RoutedMsg):
            self.routeMessage(msg, sender)


class targetTests:

    def test101_TargetActorsDirect(self):
        # Simple verification that individual Target Actors will
        # respond to a message as expected
        system = ActorSystem()
        tgt1 = system.createActor(Target)
        tgt2 = system.createActor(Target)
        self.assertNotEqual(tgt1, tgt2)
        self.assertEqual(system.ask(tgt1, 'hello', 0.25), 'TGT %s got hello'%tgt1)
        self.assertEqual(system.ask(tgt2, 'hello', 0.25), 'TGT %s got hello'%tgt2)
        self.assertEqual(system.ask(tgt1, 'bye',   0.25), 'TGT %s got bye'%tgt1)
        self.assertEqual(system.ask(tgt2, "c'ya",  0.25), "TGT %s got c'ya"%tgt2)

    def test102_TargetActorsRouting(self):
        # Check that Target Actor will handle the RoutedMsg correctly
        # by routing to another TargetActor.
        system = ActorSystem()
        tgt1 = system.createActor(Target)
        tgt2 = system.createActor(Target)
        self.assertNotEqual(tgt1, tgt2)
        msg = RoutedMsg([tgt2])
        self.assertEqual(system.ask(tgt1, msg, 0.25).response,
                         [ (tgt1, 'passed along with 0 left'),
                           (tgt2, 'Ended chain'),
                         ])

    def test103_TargetActorsRoutingAgain(self):
        # Check that Target Actors can respond to more than one
        # RoutedMsg correctly and that previous RoutedMsg handling
        # does not negatively affect handling of future messages.
        system = ActorSystem()
        tgt1 = system.createActor(Target)
        tgt2 = system.createActor(Target)
        self.assertNotEqual(tgt1, tgt2)

        msg = RoutedMsg([tgt2])
        self.assertEqual(system.ask(tgt1, msg, 0.25).response,
                         [ (tgt1, 'passed along with 0 left'),
                           (tgt2, 'Ended chain'),
                         ])

        msg2 = RoutedMsg([tgt1])
        self.assertEqual(system.ask(tgt2, msg2, 0.25).response,
                         [ (tgt2, 'passed along with 0 left'),
                           (tgt1, 'Ended chain'),
                         ])

        msg3 = RoutedMsg([tgt2])
        self.assertEqual(system.ask(tgt1, msg3, 0.25).response,
                         [ (tgt1, 'passed along with 0 left'),
                           (tgt2, 'Ended chain'),
                         ])

    def test104_TargetActorsRoutingBounceback(self):
        # Check that routing a single message can revisit Actors they
        # have previously visited.
        system = ActorSystem()
        tgt1 = system.createActor(Target)
        tgt2 = system.createActor(Target)
        self.assertNotEqual(tgt1, tgt2)

        msg = RoutedMsg([tgt2, tgt1, tgt2, tgt1][::-1])
        self.assertEqual(system.ask(tgt1, msg, 0.25).response,
                         [ (tgt1, 'passed along with 3 left'),
                           (tgt2, 'passed along with 2 left'),
                           (tgt1, 'passed along with 1 left'),
                           (tgt2, 'passed along with 0 left'),
                           (tgt1, 'Ended chain'),
                         ])

    def test105_TargetActorsRoutingToSelf(self):
        # Check that routing a single message can revisit the same Actor repeatedly.
        system = ActorSystem()
        tgt1 = system.createActor(Target)
        tgt2 = system.createActor(Target)
        self.assertNotEqual(tgt1, tgt2)

        msg = RoutedMsg([tgt2, tgt2, tgt2, tgt1][::-1])
        self.assertEqual(system.ask(tgt1, msg, 0.25).response,
                         [ (tgt1, 'passed along with 3 left'),
                           (tgt2, 'passed along with 2 left'),
                           (tgt2, 'passed along with 1 left'),
                           (tgt2, 'passed along with 0 left'),
                           (tgt1, 'Ended chain'),
                         ])


class generatorTests:

    def test201_GeneratorActors(self):
        # Verify that Generator Actors handle simple messages similar
        # to Target Actors
        system = ActorSystem()
        gen1 = system.createActor(Generator)
        gen2 = system.createActor(Generator)
        self.assertNotEqual(gen1, gen2)
        self.assertEqual(system.ask(gen1, 'hello', 0.25), 'GEN %s got hello'%gen1)
        self.assertEqual(system.ask(gen2, 'hello', 0.25), 'GEN %s got hello'%gen2)
        self.assertEqual(system.ask(gen1, 'bye',   0.25), 'GEN %s got bye'%gen1)
        self.assertEqual(system.ask(gen2, "c'ya",  0.25), "GEN %s got c'ya"%gen2)
        
    def test202_GeneratorActorsRoutingToSelf(self):
        # Check that Generator Actors can handle routing messages,
        # including returning to the same Actor multiple times and
        # routing directly to self (summarizes test102 through test105
        # from TargetActors)..
        system = ActorSystem()
        tgt1 = system.createActor(Generator)
        tgt2 = system.createActor(Generator)
        self.assertNotEqual(tgt1, tgt2)

        msg = RoutedMsg([tgt2, tgt2, tgt2, tgt1][::-1])
        self.assertEqual(system.ask(tgt1, msg, 0.25).response,
                         [ (tgt1, 'passed along with 3 left'),
                           (tgt2, 'passed along with 2 left'),
                           (tgt2, 'passed along with 1 left'),
                           (tgt2, 'passed along with 0 left'),
                           (tgt1, 'Ended chain'),
                         ])

    def test203_GeneratorCreateTargets(self):
        # Verify a Generator can create a Target and pass messages to
        # that target.
        system = ActorSystem()
        gen1 = system.createActor(Generator)
        tgt1 = system.ask(gen1, 'Generate', 0.25)
        self.assertIsNotNone(tgt1)
        self.assertNotEqual(gen1, tgt1)
        self.assertEqual(system.ask(tgt1, 'hello', 0.25), 'TGT %s got hello'%tgt1)
        self.assertEqual(system.ask(gen1, 'hello', 0.25), 'TGT %s got hello'%tgt1)

    def test204_GeneratorCreateTargets(self):
        # Verify multiple Generators and created Targets can pass and are unique.
        system = ActorSystem()
        gen1 = system.createActor(Generator)
        gen2 = system.createActor(Generator)
        tgt1 = system.ask(gen1, 'Generate', 0.25)
        tgt2 = system.ask(gen2, 'Generate', 0.25)
        self.assertIsNotNone(tgt1)
        self.assertIsNotNone(tgt2)
        self.assertNotEqual(gen1, gen2)
        self.assertNotEqual(gen1, tgt1)
        self.assertNotEqual(gen1, tgt2)
        self.assertNotEqual(gen2, tgt1)
        self.assertNotEqual(gen2, tgt2)
        self.assertNotEqual(tgt1, tgt2)
        self.assertEqual(system.ask(tgt1, 'hello', 0.25), 'TGT %s got hello'%tgt1)
        self.assertEqual(system.ask(gen1, 'hello', 0.25), 'TGT %s got hello'%tgt1)
        self.assertEqual(system.ask(tgt2, 'howdy', 0.25), 'TGT %s got howdy'%tgt2)
        self.assertEqual(system.ask(gen2, 'howdy', 0.25), 'TGT %s got howdy'%tgt2)

    def test205_GeneratorCreateTargets(self):
        # Verify RoutedMsg between multiple Generators and Targets
        system = ActorSystem()
        gen1 = system.createActor(Generator)
        gen2 = system.createActor(Generator)
        tgt1 = system.ask(gen1, 'Generate', 0.25)
        tgt2 = system.ask(gen2, 'Generate', 0.25)
        msg = RoutedMsg([gen1,tgt2,gen2,tgt2,tgt1,gen1][::-1])
        self.assertEqual(system.ask(tgt1, msg, 0.25).response,
                         [ (tgt1, 'passed along with 5 left'),
                           (gen1, 'passed along with 4 left'),
                           (tgt2, 'passed along with 3 left'),
                           (gen2, 'passed along with 2 left'),
                           (tgt2, 'passed along with 1 left'),
                           (tgt1, 'passed along with 0 left'),
                           (gen1, 'Ended chain'),
                         ])


class TestASimpleSystem(ActorSystemTestCase, targetTests, generatorTests):
    testbase='Simple'
    scope='func'

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

