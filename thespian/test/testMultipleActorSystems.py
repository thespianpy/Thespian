import unittest
from time import sleep
import logging
import thespian.test.helpers
from thespian.test import ActorSystemTestCase
from thespian.actors import *
from multiprocessing import Process, Pipe


def wait_for_things_to_happen():
    sleep(0.08)


class ActorsLikeDogs(Actor):
    def __init__(self, *args, **kw):
        super(ActorsLikeDogs, self).__init__(*args, **kw)
        self.bestFriend = None
        self.waiter = None

    def receiveMessage(self, msg, sender):
        if msg == 'Best Friend':
            if not self.bestFriend:
                self.bestFriend = self.createActor(Lassie)
            self.send(self.bestFriend, msg)
            return

        if msg == 'Best Friend Says':
            if not self.bestFriend:
                self.send(sender, 'no best friend')
            else:
                self.waiter = sender
                self.send(self.bestFriend, 'what')
        elif self.bestFriend and self.bestFriend == sender:
            if self.waiter:
                if sender != self.bestFriend:
                    self.send(self.waiter, "ERROR: lcl/rmt address comparison not epimorphic.")
                else:
                    self.send(self.waiter, msg)
                self.waiter = None
            else:
                pass # discard message from bestFriend; normal for this test
        else:
            self.send(sender, 'Greetings.')

class AskFriendsMsg:
    def __init__(self, query, otherFriend, onBehalfOf):
        self.otherFriend = otherFriend
        self.query = query
        self.asker = onBehalfOf

class Pattinson(ActorsLikeDogs):
    def __init__(self, *args, **kw):
        super(Pattinson, self).__init__(*args, **kw)
        self.girlFriend = None
        self.asker = None

    def receiveMessage(self, msg, sender):
        logger = logging.getLogger('Thespian.Actor')
        logger.setLevel(logging.DEBUG)
        logger.debug('%s Pattinson got message "%s" from %s', str(self.myAddress),
                                                            str(msg), str(sender))
        if msg == 'girlfriend?':
            logger.debug('Pattinson creating girlfriend')
            self.girlFriend = self.createActor(Stewart)
            logger.debug('Pattinson returning girlfriend address')
            self.send(sender, self.girlFriend)
            logger.debug('Pattinson girlfriend stuff done')
        elif msg == 'allfriends?':
            friend1 = self.createActor(Jolie)
            friend2 = self.createActor(Stewart)
            logger.debug('%s Pattinson created friends %s and %s', str(self.myAddress),
                         str(friend1), str(friend2))
            self.send(friend2, AskFriendsMsg('all say?', friend1, sender))
        elif isinstance(msg, AskFriendsMsg):
            self.send(msg.asker, 'Pattinson:hi.'+msg.response)
            self.send(msg.otherFriend, ActorExitRequest())
            self.send(sender, ActorExitRequest())
        elif msg == 'Girlfriend Says':
            if not self.girlFriend:
                self.send(sender, 'no girlfriend')
            else:
                self.asker = sender
                self.send(self.girlFriend, 'you say?')
        elif msg == 'Girlfriend Best Friend Says':
            if not self.girlFriend:
                self.send(sender, 'no girlfriend')
            else:
                self.asker = sender
                self.send(self.girlFriend, 'Best Friend Says')
        elif sender == self.girlFriend:
            if self.asker:
                self.send(self.asker, 'She says ' + msg)
                self.asker = None
        else:
            super(Pattinson, self).receiveMessage(msg, sender)


class Stewart(ActorsLikeDogs):
    "This Actor requires a Humanitarian ActorSystem for execution"

    @staticmethod
    def actorSystemCapabilityCheck(capabilities, actorRequirements):
        return 'Humanitarian' in capabilities

    def __init__(self, *args, **kw):
        self.nmessages = 0
        self.cat = None
        self.askedBy = None
        super(Stewart, self).__init__(*args, **kw)

    def receiveMessage(self, msg, sender):
        logger = logging.getLogger('Thespian.Actor')
        logger.debug('Stewart got message "%s" from %s', str(msg), str(sender))
        if msg == 'you say?':
            self.send(sender, 'hi')
        elif msg == 'cat says':
            if not self.cat:
                self.cat = self.createActor(Morris)
            self.askedBy = sender
            self.send(self.cat, 'kitty')
        elif sender == self.cat:
            if self.askedBy:
                self.send(self.askedBy, msg)
                self.askedBy = None
        elif isinstance(msg, AskFriendsMsg):
            msg.backto = sender
            msg.response = 'Stewart:hello'
            self.send(msg.otherFriend, msg)
        else:
            self.nmessages = self.nmessages + 1
            if self.nmessages < 10:
                super(Stewart, self).receiveMessage(msg, sender)
            else:
                self.send(sender, 'Appear to be in infinite sending loop!')


class Jolie(Actor):
    @staticmethod
    def actorSystemCapabilityCheck(capabilities, actorRequirements):
        if actorRequirements == 'Parent':
            return 'Humanitarian' in capabilities and 'Adoption' in capabilities
        return 'Humanitarian' in capabilities
    def __init__(self, *args, **kw):
        super(Jolie, self).__init__(*args, **kw)
    def receiveMessage(self, msg, sender):
        logger = logging.getLogger('Thespian.Actor')
        logger.debug('Jolie got message "%s" from %s', str(msg), str(sender))
        if isinstance(msg, AskFriendsMsg):
            msg.response += 'Jolie:Bonjour'
            self.send(msg.backto, msg)
        else:
            self.send(sender, 'Yes, '+msg)


class Lassie(Actor):
    "This Actor requires an ActorSystem with dog food capabilities for execution"

    @staticmethod
    def actorSystemCapabilityCheck(capabilities, actorRequirements):
        return capabilities.get('dog', 'nope') == 'food'

    def receiveMessage(self, msg, sender):
        self.send(sender, 'woof!')


class Morris(Actor):
    def receiveMessage(self, msg, sender): self.send(sender, 'Meow')


class Kilmer(Actor):
    @staticmethod
    def actorSystemCapabilityCheck(capabilities, actorRequirements):
        return False  # never satisfied


class TestSolitaryActorSystem(unittest.TestCase):
    "These tests run with only the primary ActorSystem enabled."

    testbase='MultiprocTCP'
    scope='func'

    def setUp(self):
        ActorSystem('multiprocTCPBase',
                    capabilities={'dog':'food'},
                    logDefs = ActorSystemTestCase.getDefaultTestLogging())

    def tearDown(self):
        ActorSystem().shutdown()

    def test01_SimpleActorCommunication(self):
        # Actor is created and responds.  Actor has no issues with ActorSystem.
        pattinson = ActorSystem().createActor(Pattinson)
        self.assertEqual(ActorSystem().ask(pattinson, 'hello', 0.5), 'Greetings.')

    def test02_CapabilityActorCommunication(self):
        # Actor is created and responds.  Actor is compatible with ActorSystem.
        lassie = ActorSystem().createActor(Lassie)
        self.assertEqual(ActorSystem().ask(lassie, 'hello', 0.5), 'woof!')

    def test03_NoCommunicationToIntransigentActor(self):
        # Actor can never be created... check proper exception is thrown
        self.assertRaisesRegex(NoCompatibleSystemForActor,
                               '.*No (compatible ActorSystem|RemoteActorCreated).*Kilmer.*',
                               ActorSystem().createActor, Kilmer)

    def test04_NoCommunicationToInvalidConstraintActor(self):
        # Actor cannot be created in primary ActorSystem; no other ActorSystem for Actor
        self.assertRaisesRegex(NoCompatibleSystemForActor,
                               '.*No (compatible ActorSystem|RemoteActorCreated).*Stewart.*',
                               ActorSystem().createActor, Stewart)


class TestSolitaryMPQueueActorSystem(TestSolitaryActorSystem):
    testbase='MultiprocQueue'
    scope='func'

    def setUp(self):
        ActorSystem('multiprocQueueBase',
                    capabilities={'dog':'food'},
                    logDefs = ActorSystemTestCase.getDefaultTestLogging())


class TestSolitaryMPUDPActorSystem(TestSolitaryActorSystem):
    testbase='MultiprocUDP'
    scope='func'

    def setUp(self):
        ActorSystem('multiprocUDPBase',
                    capabilities={'dog':'food'},
                    logDefs = ActorSystemTestCase.getDefaultTestLogging())


def System2(conn, base, capabilities):
    ActorSystem().shutdown()  # shutdown anything still running
    sleep(0.05)  # give ConventionLeader in main process time to startup
    ActorSystem(base,
                capabilities,
                logDefs = ActorSystemTestCase.getDefaultTestLogging())
    conn.recv()  # wait for shutdown indication from parent
    ActorSystem().shutdown()
    conn.close()


class TestMultiProcessSystem(unittest.TestCase):
    testbase='MultiprocTCP'
    scope='func'

    """These tests run the primary ActorSystem locally and the System2
    Actorsystem in a separate process."""

    portBase = 0
    baseName = 'multiprocTCPBase'

    def setUp(self):
        pass

    def startSystems(self, portOffset):
        system1Port = 43000 + self.portBase + portOffset
        system2Port = 42000 + self.portBase + portOffset
        self.system1Caps = {'dog':'food',
                            'Admin Port': system1Port,
                            #'Convention Address': not specified, but is the leader anyhow
        }

        self.parent_conn, child_conn = Pipe()
        self.child = Process(target=System2,
                             args=(child_conn,
                                   self.baseName,
                                   { 'Admin Port': system2Port,
                                     'Convention Address.IPv4': ('localhost:%d'%system1Port),
                                     'Humanitarian': True,
                                     'Adoption': True},
                               ))
        self.child.start()
        ActorSystem(self.baseName,
                    capabilities = self.system1Caps,
                    logDefs = ActorSystemTestCase.getDefaultTestLogging())
        sleep(0.15)  # allow System2 to start and join the Convention

    def tearDown(self):
        ActorSystem().shutdown()
        if hasattr(self, 'parent_conn'):
            self.parent_conn.send('OK, all done')
        if hasattr(self, 'child'):
            self.child.join()

    def test00_nothing(self):
        # Verifies ActorSystems can start and stop... nothing else happening.
        self.startSystems(0)
        pass

    def test01_SimpleActor(self):
        # Actor is created, but does nothing.  Multiple ActorSystems should complete registration.
        self.startSystems(1)
        pattinson = ActorSystem().createActor(Pattinson)

    def test02_SimpleActorMatchPrimaryCapabilities(self):
        # Actor is created, validating against primary ActorSystem
        # capabilities, but does nothing.  Multiple ActorSystems
        # should complete registration.
        self.startSystems(2)
        lassie = ActorSystem().createActor(Lassie)

    def test03_SimpleActorMatchOtherSystemCapabilities(self):
        # Actor creation fails validation against primary ActorSystem
        # capabilities, but matches alternate system capabilities.
        # Actor does nothing.
        self.startSystems(3)
        stewart = ActorSystem().createActor(Stewart)
        # Immediate Exit
        #n.b. the ActorSystem() will shutdown while Stewart is not yet
        #setup (PendingActor state).  This will leave Stewart behind.
        #Eventually, Stewart will get bored to death and exit anyhow.

    def test04_SimpleActorMatchOtherSystemCapabilitiesWithDelay(self):
        # Actor creation fails validation against primary ActorSystem
        # capabilities, but matches alternate system capabilities.
        # Actor does nothing.
        self.startSystems(4)
        stewart = ActorSystem().createActor(Stewart)
        wait_for_things_to_happen()
        # No good way to verify at this stage...

    def test05_LocalActorSend(self):
        # Actor is created in Convention Leader actor system and sent a message.
        self.startSystems(5)
        pattinson = ActorSystem().createActor(Pattinson)
        ActorSystem().tell(pattinson, 'hello')

    def test06_RemoteActorSend(self):
        # Actor is created in remote actor system and sent a message.
        self.startSystems(6)
        stewart = ActorSystem().createActor(Stewart)
        ActorSystem().tell(stewart, 'hello')
        #n.b. the ActorSystem() will shutdown while Stewart is not yet
        #setup (PendingActor state).  This will leave Stewart behind.
        #Eventually, Stewart will get bored to death and exit anyhow.

    def test07_0_RemoteActorSendWithDelay(self):
        # Actor is created in remote actor system and sent a message.
        self.startSystems(7)
        stewart = ActorSystem().createActor(Stewart)
        ActorSystem().tell(stewart, 'hello')
        wait_for_things_to_happen()
        # No good way to verify at this stage...

    def test07_1_RemoteActorSendAndExitWithDelay(self):
        # Actor is created in remote actor system and sent a message.
        self.startSystems(8)
        stewart = ActorSystem().createActor(Stewart)
        ActorSystem().tell(stewart, 'hello')
        wait_for_things_to_happen()
        ActorSystem().tell(stewart, 'hello')
        ActorSystem().tell(stewart, 'hello')
        wait_for_things_to_happen()

    def test07_2_RemoteActorSendAndExitWithDelay(self):
        # Actor is created in remote actor system and sent a message.
        self.startSystems(9)
        stewart = ActorSystem().createActor(Stewart)
        ActorSystem().tell(stewart, 'hello')
        wait_for_things_to_happen()
        ActorSystem().tell(stewart, ActorExitRequest())
        wait_for_things_to_happen()
        # No good way to verify at this stage...

    def test08_0_CreateTwoActorsSeparateSystems(self):
        self.startSystems(10)
        pattinson = ActorSystem().createActor(Pattinson)
        stewart = ActorSystem().createActor(Stewart)  # should be created in System2
        ActorSystem().tell(pattinson, 'hello')
        ActorSystem().tell(stewart, 'hello')
        wait_for_things_to_happen()
        # No good way to verify at this stage...

    def test08_1_CreateTwoActorsSeparateSystemsWithExplicitShutdown(self):
        self.startSystems(11)
        pattinson = ActorSystem().createActor(Pattinson)
        stewart = ActorSystem().createActor(Stewart)  # should be created in System2
        ActorSystem().tell(pattinson, 'hello')
        ActorSystem().tell(stewart, 'hello')
        wait_for_things_to_happen()
        ActorSystem().tell(pattinson, ActorExitRequest())
        ActorSystem().tell(stewart, ActorExitRequest())
        wait_for_things_to_happen()
        # No good way to verify at this stage...

    def test09_LocalActorAsk(self):
        # Actor is created in Convention Leader actor system and sent a message.
        self.startSystems(12)
        pattinson = ActorSystem().createActor(Pattinson)
        self.assertEqual(ActorSystem().ask(pattinson, 'hello'), 'Greetings.')

    def test10_RemoteActorAsk(self):
        # Actor is created in remote actor system and sent a message.
        self.startSystems(13)
        stewart = ActorSystem().createActor(Stewart)
        self.assertEqual(ActorSystem().ask(stewart, 'hello', 10), 'Greetings.')

    def test11_UnsupportableActorAsk(self):
        # Actor cannot be created, ask should timeout
        self.startSystems(14)
        self.assertRaisesRegex(NoCompatibleSystemForActor,
                               '.*No (compatible ActorSystem|RemoteActorCreated).*Kilmer.*',
                               ActorSystem().createActor, Kilmer)

    def test12_CreateLocalChildActor(self):
        self.startSystems(15)
        pattinson = ActorSystem().createActor(Pattinson)
        ActorSystem().tell(pattinson, 'Best Friend')
        self.assertEqual(ActorSystem().ask(pattinson, 'Best Friend Says', 2), 'woof!')

    def test13_CreateLocalChildOfRemoteActor(self):
        self.startSystems(16)
        stewart = ActorSystem().createActor(Stewart)
        ActorSystem().tell(stewart, 'Best Friend')
        self.assertEqual(ActorSystem().ask(stewart, 'Best Friend Says', 2), 'woof!')
        self.assertEqual(ActorSystem().ask(stewart, 'hello', 2), 'Greetings.')
        self.assertEqual(ActorSystem().ask(stewart, 'Best Friend Says', 2), 'woof!')
        wait_for_things_to_happen()
        ActorSystem().tell(stewart, ActorExitRequest())
        wait_for_things_to_happen()
        # No good way to verify at this stage...

    def test14_CreateRemoteChildOfRemoteActor(self):
        self.startSystems(17)
        stewart = ActorSystem().createActor(Stewart)
        ActorSystem().tell(stewart, 'Best Friend')
        self.assertEqual(ActorSystem().ask(stewart, 'Best Friend Says', 2), 'woof!')
        self.assertEqual(ActorSystem().ask(stewart, 'cat says', 2), 'Meow')
        self.assertEqual(ActorSystem().ask(stewart, 'hello', 2), 'Greetings.')
        self.assertEqual(ActorSystem().ask(stewart, 'cat says', 2), 'Meow')
        self.assertEqual(ActorSystem().ask(stewart, 'Best Friend Says', 2), 'woof!')
        wait_for_things_to_happen()
        # No good way to verify at this stage...

    def test15_LocalToRemoteToLocal(self):
        self.startSystems(18)
        pattinson = ActorSystem().createActor(Pattinson)
        self.assertEqual(ActorSystem().ask(pattinson, 'hello',2 ), 'Greetings.')
        self.assertEqual(ActorSystem().ask(pattinson, 'Girlfriend Says', 2), 'no girlfriend')
        stewart = ActorSystem().ask(pattinson, 'girlfriend?', 2)
        self.assertEqual(ActorSystem().ask(stewart, 'hello', 2), 'Greetings.')
        ActorSystem().tell(pattinson, 'Best Friend')
        self.assertEqual(ActorSystem().ask(stewart, 'Best Friend Says', 2), 'no best friend')
        ActorSystem().tell(stewart, 'Best Friend')
        self.assertEqual(ActorSystem().ask(stewart, 'Best Friend Says', 2), 'woof!')
        self.assertEqual(ActorSystem().ask(pattinson, 'Girlfriend Says', 2), 'She says hi')
        self.assertEqual(ActorSystem().ask(pattinson, 'Girlfriend Best Friend Says', 2), 'She says woof!')
        ActorSystem().tell(stewart, ActorExitRequest())
        wait_for_things_to_happen()
        # No good way to verify at this stage...

    def test16_DistributedActorFamily(self):
        self.startSystems(19)
        pattinson = ActorSystem().createActor(Pattinson)
        stewart = ActorSystem().ask(pattinson, 'girlfriend?', 2)
        ActorSystem().tell(stewart, 'Best Friend')
        ActorSystem().tell(pattinson, 'Best Friend')
        self.assertEqual(ActorSystem().ask(stewart, 'cat says', 2), 'Meow')

        self.assertEqual(ActorSystem().ask(pattinson, 'hello', 2), 'Greetings.')
        self.assertEqual(ActorSystem().ask(stewart, 'hello', 2), 'Greetings.')
        ActorSystem().tell(pattinson, 'Best Friend')
        self.assertEqual(ActorSystem().ask(stewart, 'Best Friend Says', 2), 'woof!')
        self.assertEqual(ActorSystem().ask(pattinson, 'Best Friend Says', 2), 'woof!')
        self.assertEqual(ActorSystem().ask(pattinson, 'Girlfriend Says', 2), 'She says hi')
        self.assertEqual(ActorSystem().ask(pattinson, 'Girlfriend Best Friend Says', 2), 'She says woof!')
        self.assertEqual(ActorSystem().ask(stewart, 'cat says', 2), 'Meow')

    def test17_CreateRemoteFriends(self):
        self.startSystems(20)
        pattinson = ActorSystem().createActor(Pattinson)
        ActorSystem().tell(pattinson, 'Best Friend')
        self.assertEqual(ActorSystem().ask(pattinson, 'Best Friend Says', 2), 'woof!')
        self.assertEqual(ActorSystem().ask(pattinson, 'Hello', 2), 'Greetings.')

        self.assertEqual(ActorSystem().ask(pattinson, 'allfriends?', 2),
                         'Pattinson:hi.Stewart:helloJolie:Bonjour')
        self.assertEqual(ActorSystem().ask(pattinson, 'Goodbye', 2), 'Greetings.')

    def test18_CreateWithTargetActorRequirements(self):
        self.startSystems(21)
        jolie = ActorSystem().createActor(Jolie, 'Parent')
        self.assertEqual(ActorSystem().ask(jolie, 'Hello', 2), 'Yes, Hello')


class TestMPUDPSystem(TestMultiProcessSystem):
    testbase='MultiprocUDP'
    scope='func'
    portBase = 100
    baseName = 'multiprocUDPBase'
