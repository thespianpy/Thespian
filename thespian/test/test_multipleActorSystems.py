import pytest
from pytest import raises
from time import sleep
import logging
from thespian.test import *
from thespian.actors import *
from datetime import timedelta


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
            if sender != self.myAddress:
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


class TestFuncSolitaryActorSystem(object):
    "These tests run with only the primary ActorSystem enabled."

    def test01_SimpleActorCommunication(self, asys):
        actor_system_unsupported(asys, 'simpleSystemBase')
        # Actor is created and responds.  Actor has no issues with ActorSystem.
        pattinson = asys.createActor(Pattinson)
        assert asys.ask(pattinson, 'hello', 0.5) == 'Greetings.'

    def test02_CapabilityActorCommunication(self, asys):
        actor_system_unsupported(asys, 'simpleSystemBase')
        # Actor is created and responds.  Actor is compatible with ActorSystem.
        lassie = asys.createActor(Lassie)
        assert asys.ask(lassie, 'hello', 0.5) == 'woof!'

    def test03_NoCommunicationToIntransigentActor(self, asys):
        actor_system_unsupported(asys, 'simpleSystemBase')
        # Actor can never be created... check proper exception is thrown
        with raises(NoCompatibleSystemForActor) as excinfo:
            asys.createActor(Kilmer)
        assert 'No compatible ActorSystem' in str(excinfo)
        assert 'Kilmer' in str(excinfo)

    def test04_NoCommunicationToInvalidConstraintActor(self, asys):
        actor_system_unsupported(asys, 'simpleSystemBase')
        # Actor cannot be created in primary ActorSystem; no other ActorSystem for Actor
        with raises(NoCompatibleSystemForActor) as excinfo:
            asys.createActor(Stewart)
        assert 'Stewart' in str(excinfo)
        assert 'No compatible ActorSystem' in str(excinfo)


class PreRegActor(ActorTypeDispatcher):
    def receiveMsg_str(self, regaddr, sender):
        self.preRegisterRemoteSystem(regaddr, {})
        self.send(sender, 'Registered')


@pytest.fixture
def testsystems(request, asys):
    asys2 = similar_asys(asys, in_convention=not asys.txonly,
                         capabilities = { 'Humanitarian': True,
                                          'Adoption': True
                         })
    request.addfinalizer(lambda asys2=asys2: asys2.shutdown())
    sleep(0.15)  # allow System2 to start and join the Convention
    if asys.txonly:
        assert 'Registered' == asys.ask(asys.createActor(PreRegActor),
                                        'localhost:%d'%asys2.port_num,
                                        timedelta(seconds=3))
        sleep(0.25)  # allow System2 to join the Convention
    return asys, asys2


class TestFuncMultiProcessSystem(object):
    """These tests run the primary ActorSystem locally and the System2
    Actorsystem in a separate process."""

    def test00_nothing(self, testsystems):
        # Verifies ActorSystems can start and stop... nothing else happening.
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        pass

    def test01_SimpleActor(self, testsystems):
        # Actor is created, but does nothing.  Multiple ActorSystems
        # should complete registration.
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        pattinson = asys.createActor(Pattinson)

    def test02_SimpleActorMatchPrimaryCapabilities(self, testsystems):
        # Actor is created, validating against primary ActorSystem
        # capabilities, but does nothing.  Multiple ActorSystems
        # should complete registration.
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        lassie = asys.createActor(Lassie)

    def test03_SimpleActorMatchOtherSystemCapabilities(self, testsystems):
        # Actor creation fails validation against primary ActorSystem
        # capabilities, but matches alternate system capabilities.
        # Actor does nothing.
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        stewart = asys.createActor(Stewart)
        # Immediate Exit
        #n.b. the asys will shutdown while Stewart is not yet
        #setup (PendingActor state).  This will leave Stewart behind.
        #Eventually, Stewart will get bored to death and exit anyhow.

    def test04_SimpleActorMatchOtherSystemCapabilitiesWithDelay(self, testsystems):
        # Actor creation fails validation against primary ActorSystem
        # capabilities, but matches alternate system capabilities.
        # Actor does nothing.
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        stewart = asys.createActor(Stewart)
        wait_for_things_to_happen()
        # No good way to verify at this stage...

    def test05_LocalActorSend(self, testsystems):
        # Actor is created in Convention Leader actor system and sent a message.
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        pattinson = asys.createActor(Pattinson)
        asys.tell(pattinson, 'hello')

    def test06_RemoteActorSend(self, testsystems):
        # Actor is created in remote actor system and sent a message.
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        stewart = asys.createActor(Stewart)
        asys.tell(stewart, 'hello')
        #n.b. the ActorSystem will shutdown while Stewart is not yet
        #setup (PendingActor state).  This will leave Stewart behind.
        #Eventually, Stewart will get bored to death and exit anyhow.

    def test07_0_RemoteActorSendWithDelay(self, testsystems):
        # Actor is created in remote actor system and sent a message.
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        stewart = asys.createActor(Stewart)
        asys.tell(stewart, 'hello')
        wait_for_things_to_happen()
        # No good way to verify at this stage...

    def test07_1_RemoteActorSendAndExitWithDelay(self, testsystems):
        # Actor is created in remote actor system and sent a message.
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        stewart = asys.createActor(Stewart)
        asys.tell(stewart, 'hello')
        wait_for_things_to_happen()
        asys.tell(stewart, 'hello')
        asys.tell(stewart, 'hello')
        wait_for_things_to_happen()

    def test07_2_RemoteActorSendAndExitWithDelay(self, testsystems):
        # Actor is created in remote actor system and sent a message.
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        stewart = asys.createActor(Stewart)
        asys.tell(stewart, 'hello')
        wait_for_things_to_happen()
        asys.tell(stewart, ActorExitRequest())
        wait_for_things_to_happen()
        # No good way to verify at this stage...

    def test08_0_CreateTwoActorsSeparateSystems(self, testsystems):
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        pattinson = asys.createActor(Pattinson)
        stewart = asys.createActor(Stewart)  # should be created in System2
        asys.tell(pattinson, 'hello')
        asys.tell(stewart, 'hello')
        wait_for_things_to_happen()
        # No good way to verify at this stage...

    def test08_1_CreateTwoActorsSeparateSystemsWithExplicitShutdown(self, testsystems):
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        pattinson = asys.createActor(Pattinson)
        stewart = asys.createActor(Stewart)  # should be created in System2
        asys.tell(pattinson, 'hello')
        asys.tell(stewart, 'hello')
        wait_for_things_to_happen()
        asys.tell(pattinson, ActorExitRequest())
        asys.tell(stewart, ActorExitRequest())
        wait_for_things_to_happen()
        # No good way to verify at this stage...

    def test09_LocalActorAsk(self, testsystems):
        # Actor is created in Convention Leader actor system and sent a message.
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        pattinson = asys.createActor(Pattinson)
        r = asys.ask(pattinson, 'hello')
        assert r == 'Greetings.'

    def test10_RemoteActorAsk(self, testsystems):
        # Actor is created in remote actor system and sent a message.
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        stewart = asys.createActor(Stewart)
        r = asys.ask(stewart, 'hello', 10)
        assert r == 'Greetings.'

    def test11_UnsupportableActorAsk(self, testsystems):
        # Actor cannot be created, ask should timeout
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        with raises(NoCompatibleSystemForActor) as excinfo:
            asys.createActor(Kilmer)
        assert 'No compatible ActorSystem' in str(excinfo)
        assert 'Kilmer' in str(excinfo)

    def test12_CreateLocalChildActor(self, testsystems):
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        pattinson = asys.createActor(Pattinson)
        asys.tell(pattinson, 'Best Friend')
        r = asys.ask(pattinson, 'Best Friend Says', 2)
        assert r == 'woof!'

    def test13_CreateLocalChildOfRemoteActor(self, testsystems):
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        stewart = asys.createActor(Stewart)
        asys.tell(stewart, 'Best Friend')
        r = asys.ask(stewart, 'Best Friend Says', 2)
        assert r == 'woof!'
        r = asys.ask(stewart, 'hello', 2)
        assert r == 'Greetings.'
        r = asys.ask(stewart, 'Best Friend Says', 2)
        assert r == 'woof!'
        wait_for_things_to_happen()
        asys.tell(stewart, ActorExitRequest())
        wait_for_things_to_happen()
        # No good way to verify at this stage...

    def test14_CreateRemoteChildOfRemoteActor(self, testsystems):
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        stewart = asys.createActor(Stewart)
        asys.tell(stewart, 'Best Friend')
        r = asys.ask(stewart, 'Best Friend Says', 2)
        assert r == 'woof!'
        r = asys.ask(stewart, 'cat says', 2)
        assert r == 'Meow'
        r = asys.ask(stewart, 'hello', 2)
        assert r == 'Greetings.'
        r = asys.ask(stewart, 'cat says', 2)
        assert r == 'Meow'
        r = asys.ask(stewart, 'Best Friend Says', 2)
        assert r == 'woof!'
        wait_for_things_to_happen()
        # No good way to verify at this stage...

    def test15_LocalToRemoteToLocal(self, testsystems):
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        pattinson = asys.createActor(Pattinson)
        r = asys.ask(pattinson, 'hello',2 )
        assert r == 'Greetings.'
        r = asys.ask(pattinson, 'Girlfriend Says', 2)
        assert r == 'no girlfriend'
        stewart = asys.ask(pattinson, 'girlfriend?', 2)
        r = asys.ask(stewart, 'hello', 2)
        assert r == 'Greetings.'
        asys.tell(pattinson, 'Best Friend')
        r = asys.ask(stewart, 'Best Friend Says', 2)
        assert r == 'no best friend'
        asys.tell(stewart, 'Best Friend')
        r = asys.ask(stewart, 'Best Friend Says', 2)
        assert r == 'woof!'
        r = asys.ask(pattinson, 'Girlfriend Says', 2)
        assert r == 'She says hi'
        r = asys.ask(pattinson, 'Girlfriend Best Friend Says', 2)
        assert r == 'She says woof!'
        asys.tell(stewart, ActorExitRequest())
        wait_for_things_to_happen()
        # No good way to verify at this stage...

    def test16_DistributedActorFamily(self, testsystems):
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        pattinson = asys.createActor(Pattinson)
        stewart = asys.ask(pattinson, 'girlfriend?', 2)
        asys.tell(stewart, 'Best Friend')
        asys.tell(pattinson, 'Best Friend')
        r = asys.ask(stewart, 'cat says', 2)
        assert r == 'Meow'

        r = asys.ask(pattinson, 'hello', 2)
        assert r == 'Greetings.'
        r = asys.ask(stewart, 'hello', 2)
        assert r == 'Greetings.'
        asys.tell(pattinson, 'Best Friend')
        r = asys.ask(stewart, 'Best Friend Says', 2)
        assert r == 'woof!'
        r = asys.ask(pattinson, 'Best Friend Says', 2)
        assert r == 'woof!'
        r = asys.ask(pattinson, 'Girlfriend Says', 2)
        assert r == 'She says hi'
        r = asys.ask(pattinson, 'Girlfriend Best Friend Says', 2)
        assert r == 'She says woof!'
        r = asys.ask(stewart, 'cat says', 2)
        assert r == 'Meow'

    def test17_CreateRemoteFriends(self, testsystems):
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        pattinson = asys.createActor(Pattinson)
        asys.tell(pattinson, 'Best Friend')
        r = asys.ask(pattinson, 'Best Friend Says', 2)
        assert r == 'woof!'
        r = asys.ask(pattinson, 'Hello', 2)
        assert r == 'Greetings.'

        r = asys.ask(pattinson, 'allfriends?', 2)
        assert r == 'Pattinson:hi.Stewart:helloJolie:Bonjour'
        r = asys.ask(pattinson, 'Goodbye', 2)
        assert r == 'Greetings.'

    def test18_CreateWithTargetActorRequirements(self, testsystems):
        asys, asys2 = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        jolie = asys.createActor(Jolie, 'Parent')
        r = asys.ask(jolie, 'Hello', 2)
        assert r == 'Yes, Hello'
