"""Various tests that determine whether updating capabilities for
multiple ActorSystems in a Convention are working correctly.  These
tests run somewhat slowly because they must allow time for
coordination of effects an hysteresis of same between the multiple
systems (which should not be an issue under normal operations).
"""

import pytest
from pytest import raises
from thespian.test import *
import time
from thespian.actors import *


colors = ['Red', 'Blue', 'Green', 'Yellow']

class SetCap(object):
    def __init__(self, capName, capValue):
        self.capName = capName
        self.capValue = capValue


class ColorActorBase(Actor):
    """This actor has a particular color (identified by self.color), and
       requires that color to be a capability of the ActorSystem it runs in.

       If given a string message, returns it with "Got: " prefixed to
       the string.

       If given a tuple message, the tuple should be a series of
       colors (strings), ending with a text message.  It will forward
       the tuple to the sub-actor specified by the first color in the
       tuple (removing that color from the tuple); the last sub-actor
       to receive the message will send it back to the original sender
       (which was appended to the tuple by the first recipient).

    """

    def __init__(self):
        self._subs = {}
    def receiveMessage(self, msg, sender):
        if type(msg) == type("hi"):
            self.send(sender, "Got: " + msg)
        elif isinstance(msg, SetCap):
            self.updateCapability(msg.capName, msg.capValue)
            self.send(sender, 'ok')
        elif type(msg) == type((1,2)):
            if type(msg[-1]) == type(""):
                msg = tuple(list(msg) + [sender])
            if len(msg) > 2:
                fwdTo = msg[0]
                fwdMsg = tuple(list(msg)[1:])
                if fwdTo not in self._subs:
                    self._subs[fwdTo] = self.createActor(fwdTo)
                self.send(self._subs[fwdTo], fwdMsg)
            else:
                self.send(msg[1], msg[0])
        elif isinstance(msg, ChildActorExited):
            for each in self._subs:
                if self._subs[each] == msg.childAddress:
                    del self._subs[each]
                    break


class RedActor(ColorActorBase):
    @staticmethod
    def actorSystemCapabilityCheck(capabilities, actorRequirements):
        return capabilities.get('Red', False)

class GreenActor(ColorActorBase):
    @staticmethod
    def actorSystemCapabilityCheck(capabilities, actorRequirements):
        return capabilities.get('Green', False)

class BlueActor(ColorActorBase):
    @staticmethod
    def actorSystemCapabilityCheck(capabilities, actorRequirements):
        return capabilities.get('Blue', False)

class OrangeActor(ColorActorBase):
    # This actor has no actorSystemCapabilityCheck
    pass


@pytest.fixture
def asys_trio(request, asys):
    asys2 = similar_asys(asys, in_convention=True, start_wait=False)
    asys3 = similar_asys(asys, in_convention=True)
    request.addfinalizer(lambda asys2=asys2, asys3=asys3:
                         asys2.shutdown() == asys3.shutdown())
    return (asys, asys2, asys3)


class TestFuncSingleSystemCapabilityUpdates(object):

    def test00_systemUpdatable(self, asys):
        asys.updateCapability('Colors', ['Red', 'Blue', 'Green'])
        asys.updateCapability('Here', True)
        asys.updateCapability('Here')

    def test01_actorUpdatable(self, asys):
        orange = asys.createActor(OrangeActor)
        assert 'ok' == asys.ask(orange, SetCap('Blue', True), 1)



class TestFuncCapabilityUpdates(object):

    def test00_systemsRunnable(self, asys_trio):
        asys1, asys2, asys3 = asys_trio
        actor_system_unsupported(asys1, "simpleSystemBase", "multiprocQueueBase")

    def test01_defaultSystemsDoNotSupportColorActors(self, asys_trio):
        asys1, asys2, asys3 = asys_trio
        actor_system_unsupported(asys1,
                                 "simpleSystemBase",
                                 "multiprocQueueBase")
        raises(NoCompatibleSystemForActor, asys1.createActor, RedActor)
        raises(NoCompatibleSystemForActor, asys1.createActor, BlueActor)
        raises(NoCompatibleSystemForActor, asys1.createActor, GreenActor)

    def test02_addColorCapabilitiesAllowsColorActors(self, asys_trio):
        asys1, asys2, asys3 = asys_trio
        actor_system_unsupported(asys1,
                                 "simpleSystemBase",
                                 "multiprocQueueBase")
        # Setup Systems
        asys1.updateCapability('Red', True)
        asys2.updateCapability('Green', True)
        asys3.updateCapability('Blue', True)
        max_wait = 0.2
        time.sleep(max_wait*3)  # Allow for propagation (with hysteresis)
        # Create one actor in each system
        red = asys1.createActor(RedActor)
        green = asys1.createActor(GreenActor)
        blue = asys1.createActor(BlueActor)
        # Verify got valid ActorAddresses
        assert red is not None
        assert green is not None
        assert blue is not None
        assert isinstance(red, ActorAddress)
        assert isinstance(green, ActorAddress)
        assert isinstance(blue, ActorAddress)
        # Verify actors are responsive
        assert "Got: hello" == asys1.ask(red, 'hello', 1)
        assert "Got: howdy" == asys1.ask(green, 'howdy', 1)
        assert "Got: greetings" == asys1.ask(blue, 'greetings', 1)
        # Tell actors to exit
        asys1.tell(red, ActorExitRequest())
        asys1.tell(green, ActorExitRequest())
        asys1.tell(blue, ActorExitRequest())
        time.sleep(0.1)

    def test02_1_addColorCapabilitiesAllowsColorActorsAndSubActors(self, asys_trio):
        asys1, asys2, asys3 = asys_trio
        actor_system_unsupported(asys1,
                                 "simpleSystemBase",
                                 "multiprocQueueBase")
        # Setup Systems
        asys1.updateCapability('Red', True)
        asys2.updateCapability('Green', True)
        asys3.updateCapability('Blue', True)
        max_wait = 2.0
        time.sleep(max_wait)  # Allow for propagation (with hysteresis)
        # Create one actor in each system
        red = asys1.createActor(RedActor)
        green = asys1.createActor(GreenActor)
        blue = asys1.createActor(BlueActor)
        orange = asys1.createActor(OrangeActor)
        # Verify got valid ActorAddresses
        assert red is not None
        assert green is not None
        assert blue is not None
        assert orange is not None
        assert isinstance(red, ActorAddress)
        assert isinstance(green, ActorAddress)
        assert isinstance(blue, ActorAddress)
        assert isinstance(orange, ActorAddress)
        # Verify actors are responsive
        assert "Got: hello" == asys1.ask(red, 'hello', 1)
        assert "Got: howdy" == asys1.ask(green, 'howdy', 1)
        assert "Got: greetings" == asys1.ask(blue, 'greetings', 1)
        assert "Got: aloha" == asys1.ask(orange, 'aloha', 1)
        # Create a chain of multiple colors from each top level
        assert "path1" == asys1.ask(red, (BlueActor, GreenActor, RedActor,
                                          GreenActor, BlueActor, RedActor,
                                          "path1"),
                                    1)
        assert "path2" == asys1.ask(green, (BlueActor, GreenActor, RedActor,
                                            GreenActor, BlueActor, RedActor,
                                            "path2"),
                                    1)
        assert "path3" == asys1.ask(blue, (BlueActor, GreenActor, RedActor,
                                           GreenActor, OrangeActor, BlueActor,
                                           RedActor,
                                           "path3"),
                                    1)
        # Tell actors to exit
        asys1.tell(red, ActorExitRequest())
        asys1.tell(green, ActorExitRequest())
        asys1.tell(blue, ActorExitRequest())
        time.sleep(0.1)

    def test03_addMultipleColorCapabilitiesToOneActorSystemAllowsColorActors(self, asys_trio):
        asys1, asys2, asys3 = asys_trio
        actor_system_unsupported(asys1, "simpleSystemBase", "multiprocQueueBase")
        # Setup systems
        asys1.updateCapability('Red', True)
        asys2.updateCapability('Green', True)
        asys2.updateCapability('Blue', True)
        max_wait = 0.2
        time.sleep(max_wait*6)  # Allow for propagation (with hysteresis)
        # Create Actors (two in system Two)
        red = asys1.createActor(RedActor)
        green = asys1.createActor(GreenActor)
        blue = asys1.createActor(BlueActor)
        # Verify got valid ActorAddresses
        assert red is not None
        assert green is not None
        assert blue is not None
        assert isinstance(red, ActorAddress)
        assert isinstance(green, ActorAddress)
        assert isinstance(blue, ActorAddress)
        # Verify actors are responsive
        assert "Got: hello" == asys1.ask(red, 'hello', 1)
        assert "Got: howdy" == asys1.ask(green, 'howdy', 1)
        assert "Got: greetings" == asys1.ask(blue, 'greetings', 1)
        # Tell actors to exit
        asys1.tell(red, ActorExitRequest())
        asys1.tell(green, ActorExitRequest())
        asys1.tell(blue, ActorExitRequest())
        time.sleep(0.1)

    def test04_addMultipleColorCapabilitiesToLeaderActorSystemAllowsColorActors(self, asys_trio):
        asys1, asys2, asys3 = asys_trio
        actor_system_unsupported(asys1, "simpleSystemBase", "multiprocQueueBase")
        # Setup systems
        asys1.updateCapability('Red', True)
        asys1.updateCapability('Green', True)
        asys1.updateCapability('Blue', True)
        max_wait = 0.2
        time.sleep(max_wait*6)  # Allow for propagation (with hysteresis)
        # Create Actors (all in system One)
        red = asys1.createActor(RedActor)
        green = asys1.createActor('thespian.test.test_updateSystemCapabilities.GreenActor')
        blue = asys1.createActor(BlueActor)
        # Verify got valid ActorAddresses
        assert red is not None
        assert green is not None
        assert blue is not None
        assert isinstance(red, ActorAddress)
        assert isinstance(green, ActorAddress)
        assert isinstance(blue, ActorAddress)
        # Verify actors are responsive
        assert "Got: hello" == asys1.ask(red, 'hello', 1)
        assert "Got: howdy" == asys1.ask(green, 'howdy', 1)
        assert "Got: greetings" == asys1.ask(blue, 'greetings', 1)
        # Tell actors to exit
        asys1.tell(red, ActorExitRequest())
        asys1.tell(green, ActorExitRequest())
        asys1.tell(blue, ActorExitRequest())
        time.sleep(0.1)

    def test04_1_actorAddCapabilitiesEnablesOtherActors(self, asys_trio):
        asys1, asys2, asys3 = asys_trio
        actor_system_unsupported(asys1, "simpleSystemBase", "multiprocQueueBase")
        # Setup system (only one needed, because an Actor can only
        # modify its own system)
        asys1.updateCapability('Red', True)
        # Create Actors (all in system One)
        red = asys1.createActor(RedActor)
        raises(NoCompatibleSystemForActor, asys1.createActor, BlueActor)
        raises(NoCompatibleSystemForActor, asys1.createActor, GreenActor)
        orange = asys1.createActor(OrangeActor)
        # Verify actors are responsive
        assert "Got: Hello" == asys1.ask(red, 'Hello', 1)
        assert "Got: Aloha" == asys1.ask(orange, 'Aloha', 1)
        # Now have Red add a couple of capabilities
        assert 'ok' == asys1.ask(red, SetCap('Green', True), 1)
        assert 'ok' == asys1.ask(red, SetCap('Blue', True), 1)
        time.sleep(0.1)  # allow actor to process these messages
        # And create some Actors needing those capabilities
        green = asys1.createActor(GreenActor)
        blue = asys1.createActor(BlueActor)
        assert green is not None
        assert blue is not None
        assert isinstance(red, ActorAddress)
        assert isinstance(green, ActorAddress)
        assert isinstance(blue, ActorAddress)
        assert "Got: hello" == asys1.ask(red, 'hello', 1)
        assert "Got: howdy" == asys1.ask(green, 'howdy', 1)
        assert "Got: greetings" == asys1.ask(blue, 'greetings', 1)
        assert "Got: Aloha" == asys1.ask(orange, 'Aloha', 1)
        # Tell actors to exit
        asys1.tell(red, ActorExitRequest())
        asys1.tell(green, ActorExitRequest())
        asys1.tell(blue, ActorExitRequest())
        time.sleep(0.1)

    def test05_removingColorCapabilitiesKillsExistingColorActors(self, asys_trio):
        asys1, asys2, asys3 = asys_trio
        actor_system_unsupported(asys1, "simpleSystemBase", "multiprocQueueBase")
        # Setup systems
        asys1.updateCapability('Red', True)
        asys2.updateCapability('Green', True)
        asys3.updateCapability('Blue', True)
        max_wait = 0.2
        time.sleep(max_wait*3)  # Allow for propagation (with hysteresis)
        # Create Actors
        red = asys1.createActor(RedActor)
        green = asys1.createActor(GreenActor)
        blue = asys1.createActor(BlueActor)
        orange = asys1.createActor(OrangeActor)
        # Verify got valid ActorAddresses
        assert red is not None
        assert green is not None
        assert blue is not None
        assert orange is not None
        assert isinstance(red, ActorAddress)
        assert isinstance(green, ActorAddress)
        assert isinstance(blue, ActorAddress)
        assert isinstance(orange, ActorAddress)
        # Verify actors are responsive
        assert "Got: hello" == asys1.ask(red, 'hello', 1)
        assert "Got: howdy" == asys1.ask(green, 'howdy', 1)
        assert "Got: greetings" == asys1.ask(blue, 'greetings', 1)
        assert "Got: aloha" == asys1.ask(orange, 'aloha', 1)
        # Remove color capabilities from ActorSystems
        asys1.updateCapability('Red', None)
        asys2.updateCapability('Green', None)
        asys3.updateCapability('Blue', None)
        time.sleep(0.2)  # processing time allowance
        # Verify all Actors are no longer present.
        assert asys1.ask(red, '1', 1) is None
        assert asys1.ask(green, '2', 1) is None
        assert asys1.ask(blue, '3', 1) is None
        assert "Got: aloha" == asys1.ask(orange, 'aloha', 1)
        time.sleep(0.1)

    def test05_1_removingColorCapabilitiesViaActorKillsExistingColorActors(self, asys_trio):
        asys1, asys2, asys3 = asys_trio
        actor_system_unsupported(asys1, "simpleSystemBase", "multiprocQueueBase")
        # Setup systems
        asys1.updateCapability('Red', True)
        asys2.updateCapability('Green', True)
        asys3.updateCapability('Blue', True)
        max_wait = 0.2
        time.sleep(max_wait*3)  # Allow for propagation (with hysteresis)
        # Create Actors
        red = asys1.createActor(RedActor)
        green = asys1.createActor(GreenActor)
        blue = asys1.createActor(BlueActor)
        orange = asys1.createActor(OrangeActor)
        # Verify got valid ActorAddresses
        assert red is not None
        assert green is not None
        assert blue is not None
        assert orange is not None
        assert isinstance(red, ActorAddress)
        assert isinstance(green, ActorAddress)
        assert isinstance(blue, ActorAddress)
        assert isinstance(orange, ActorAddress)
        # Verify actors are responsive
        assert "Got: hello" == asys1.ask(red, 'hello', 1)
        assert "Got: howdy" == asys1.ask(green, 'howdy', 1)
        assert "Got: greetings" == asys1.ask(blue, 'greetings', 1)
        assert "Got: aloha" == asys1.ask(orange, 'aloha', 1)
        # Remove color capabilities from ActorSystems
        assert 'ok' == asys1.ask(red, SetCap('Red', False), 1)
        assert 'ok' == asys1.ask(blue, SetCap('Blue', False), 1)
        time.sleep(0.4)  # allow actor to process these messages
        # Verify affected Actors are no longer present.
        assert asys1.ask(red, '1', 1) is None
        assert "Got: Howdy" == asys1.ask(green, 'Howdy', 1)
        assert asys1.ask(blue, '3', 1) is None
        assert "Got: aloha" == asys1.ask(orange, 'aloha', 1)
        # Tell actors to exit
        asys1.tell(green, ActorExitRequest())
        asys1.tell(orange, ActorExitRequest())
        time.sleep(0.1)

    def test06_removingColorCapabilitiesPreventsNewColorActors(self, asys_trio):
        asys1, asys2, asys3 = asys_trio
        actor_system_unsupported(asys1, "simpleSystemBase", "multiprocQueueBase")
        # Setup systems
        asys1.updateCapability('Red', True)
        asys2.updateCapability('Green', True)
        asys3.updateCapability('Blue', True)
        max_wait = 0.3
        time.sleep(max_wait*6)  # Allow for propagation (with hysteresis)
        # Create Actors
        red = asys1.createActor(RedActor)
        green = asys1.createActor(GreenActor)
        blue = asys1.createActor(BlueActor)
        # Verify got valid ActorAddresses
        assert red is not None
        assert green is not None
        assert blue is not None
        assert isinstance(red, ActorAddress)
        assert isinstance(green, ActorAddress)
        assert isinstance(blue, ActorAddress)
        # Verify actors are responsive
        assert "Got: hello" == asys1.ask(red, 'hello', max_wait)
        assert "Got: howdy" == asys1.ask(green, 'howdy', max_wait)
        assert "Got: greetings" == asys1.ask(blue, 'greetings', max_wait)
        # Remove one Capability and verify that all Actors created via that ActorSystem are removed

        asys3.updateCapability('Blue', None)
        time.sleep(0.1)
        assert asys1.ask(blue, 'yono', max_wait) is None
        assert "Got: hellono" == asys1.ask(red, 'hellono', max_wait)
        assert "Got: hino" == asys1.ask(green, 'hino', max_wait)

        asys1.updateCapability('Red', None)
        time.sleep(0.1)  # wait for capability update to propagate
        assert asys1.ask(red, 'hello', max_wait) is None
        assert 'Got: hi' == asys1.ask(green, 'hi', max_wait)
        assert asys1.ask(blue, 'yo', max_wait) is None
        # Verify no Actors requiring the removed capabilities can be
        # created, but other kinds can still be created.
        raises(NoCompatibleSystemForActor, asys1.createActor, RedActor)
        red = None
        green = asys1.createActor(GreenActor)
        raises(NoCompatibleSystemForActor, asys1.createActor, BlueActor)
        # Add back the Blue capability and verify the Actor can now be created
        asys3.updateCapability('Blue', True)
        time.sleep(0.25)
        blue = asys1.createActor(BlueActor)
        assert red is None
        assert green is not None
        assert blue is not None
        assert isinstance(green, ActorAddress)
        assert isinstance(blue, ActorAddress)
        assert "Got: howdy howdy" == asys1.ask(green, 'howdy howdy', max_wait)
        assert "Got: greetings all" == asys1.ask(blue, 'greetings all',
                                                 max_wait)
        assert asys1.ask(blue, (RedActor, 'hey, red'), max_wait) is None
        assert "hey, blue" == asys1.ask(green, (BlueActor, 'hey, blue'),
                                        max_wait*10)
        assert "hey, green" == asys1.ask(blue, (GreenActor, 'hey, green'),
                                         max_wait*10)
        # Remove remaining capabilities
        asys2.updateCapability('Green', None)
        assert 'ok' == asys1.ask(blue, SetCap('Blue', None), 1)
        time.sleep(0.1)  # allow actor to process these messages
        # No new actors can be created for any color
        raises(NoCompatibleSystemForActor, asys1.createActor, RedActor)
        raises(NoCompatibleSystemForActor, asys1.createActor, BlueActor)
        raises(NoCompatibleSystemForActor, asys1.createActor, GreenActor)

    def test07_removingNonExistentCapabilitiesHasNoEffect(self, asys_trio):
        asys1, asys2, asys3 = asys_trio
        actor_system_unsupported(asys1, "simpleSystemBase", "multiprocQueueBase")
        max_wait = 1.0
        # Setup systems
        asys1.updateCapability('Red', True)
        asys2.updateCapability('Green', True)
        asys3.updateCapability('Blue', True)
        time.sleep(max_wait*2)  # Allow for propagation (with hysteresis)
        # Create Actors
        red = asys1.createActor(RedActor)
        green = asys1.createActor(GreenActor)
        blue = asys1.createActor(BlueActor)
        # Verify got valid ActorAddresses
        assert red is not None
        assert green is not None
        assert blue is not None
        assert isinstance(red, ActorAddress)
        assert isinstance(green, ActorAddress)
        assert isinstance(blue, ActorAddress)
        # Verify actors are responsive
        assert "Got: hello" == asys1.ask(red, 'hello', 1)
        assert "Got: howdy", asys1.ask(green, 'howdy' == 1)
        assert "Got: greetings" == asys1.ask(blue, 'greetings', 1)
        assert 'long path' == asys1.ask(blue, (RedActor, GreenActor, RedActor,
                                               BlueActor, GreenActor,
                                               'long path'),
                                        max_wait)
        # Verify sub-actors are responsive
        assert 'bluered' == asys1.ask(blue, (RedActor, 'bluered'), max_wait)
        assert "greenblue" == asys1.ask(green, (BlueActor, 'greenblue'),
                                        max_wait)
        assert "bluegreen" == asys1.ask(blue, (GreenActor, 'bluegreen'),
                                        max_wait)
        # Remove non-color capabilities from ActorSystems
        asys1.updateCapability('Frog', None)
        assert 'ok' == asys1.ask(blue, SetCap('Bark', None), 1)
        asys1.updateCapability('Cow', None)
        time.sleep(0.1)
        # Verify actors are still responsive
        assert "Got: hello" == asys1.ask(red, 'hello', 1)
        assert "Got: howdy" == asys1.ask(green, 'howdy', 1)
        assert "Got: greetings" == asys1.ask(blue, 'greetings', 1)
        # Verify sub-actors are still responsive
        assert 'hey, red' == asys1.ask(blue, (RedActor, 'hey, red'), max_wait)
        assert "howdy howdy" == asys1.ask(green, (BlueActor, 'howdy howdy'),
                                          max_wait)
        assert "greetings all" == asys1.ask(red, (GreenActor, 'greetings all'),
                                            max_wait)
        # Verify new sub-actors can be created
        assert 'long path' == asys1.ask(blue, (RedActor, GreenActor, RedActor,
                                               BlueActor, GreenActor,
                                               'long path'),
                                        max_wait)
        # Tell actors to exit
        asys1.tell(red, ActorExitRequest())
        asys1.tell(green, ActorExitRequest())
        asys1.tell(blue, ActorExitRequest())
        time.sleep(0.1)


    def test08_settingCapabilityToSameValueHasNoEffect(self, asys_trio):
        asys1, asys2, asys3 = asys_trio
        actor_system_unsupported(asys1, "simpleSystemBase", "multiprocQueueBase")
        max_wait = 0.9
        # Setup systems
        asys1.updateCapability('Red', True)
        asys2.updateCapability('Green', True)
        asys3.updateCapability('Blue', True)
        time.sleep(max_wait*1.8)  # Allow for propagation (with hysteresis)
        # Create Actors
        red = asys1.createActor(RedActor)
        green = asys1.createActor(GreenActor)
        blue = asys1.createActor(BlueActor)
        # Verify got valid ActorAddresses
        assert red is not None
        assert green is not None
        assert blue is not None
        assert isinstance(red, ActorAddress)
        assert isinstance(green, ActorAddress)
        assert isinstance(blue, ActorAddress)
        # Verify actors are responsive
        assert "Got: hello" == asys1.ask(red, 'hello', 1)
        assert "Got: howdy" == asys1.ask(green, 'howdy', 1)
        assert "Got: greetings" == asys1.ask(blue, 'greetings', 1)
        # Verify sub-actors are responsive
        assert 'hey, red' == asys1.ask(blue, (RedActor, 'hey, red'), max_wait)
        assert "howdy howdy" == asys1.ask(green, (GreenActor, 'howdy howdy'),
                                          max_wait)
        assert "greetings all" == asys1.ask(red, (BlueActor, 'greetings all'),
                                            max_wait)
        # Remove non-color capabilities from ActorSystems
        asys1.updateCapability('Red', True)
        asys2.updateCapability('Green', True)
        assert 'ok' == asys1.ask(blue, SetCap('Blue', True), 1)
        # Verify actors are still responsive
        assert "Got: hello" == asys1.ask(red, 'hello', 1)
        assert "Got: howdy" == asys1.ask(green, 'howdy', 1)
        assert "Got: greetings" == asys1.ask(blue, 'greetings', 1)
        # Verify sub-actors are still responsive
        assert 'hey, red' == asys1.ask(blue, (RedActor, 'hey, red'), max_wait)
        assert "howdy howdy" == asys1.ask(green, (RedActor, 'howdy howdy'),
                                          max_wait)
        assert "greetings all" == asys1.ask(red, (BlueActor, 'greetings all'),
                                            max_wait)
        # Verify new sub-actors can be created
        assert 'long path' == asys1.ask(blue, (RedActor, GreenActor, RedActor,
                                               BlueActor, GreenActor,
                                               'long path'),
                                        max_wait)
        # Tell actors to exit
        asys1.tell(red, ActorExitRequest())
        asys1.tell(green, ActorExitRequest())
        asys1.tell(blue, ActorExitRequest())
        time.sleep(0.1)


    def test09_removingCapabilityTwiceHasNoEffectTheSecondTime(self, asys_trio):
        asys1, asys2, asys3 = asys_trio
        actor_system_unsupported(asys1, "simpleSystemBase", "multiprocQueueBase")
        max_wait = 0.4
        # Setup systems
        asys1.updateCapability('Red', True)
        asys2.updateCapability('Green', True)
        asys3.updateCapability('Blue', True)
        time.sleep(max_wait*6)  # Allow for propagation (with hysteresis)
        # Create Actors
        red = asys1.createActor(RedActor)
        green = asys1.createActor('thespian.test.test_updateSystemCapabilities.GreenActor')
        blue = asys1.createActor(BlueActor)
        # Verify got valid ActorAddresses
        assert red is not None
        assert green is not None
        assert blue is not None
        assert isinstance(red, ActorAddress)
        assert isinstance(green, ActorAddress)
        assert isinstance(blue, ActorAddress)
        # Verify actors are responsive
        assert "Got: hello" == asys1.ask(red, 'hello', 1)
        assert "Got: howdy" == asys1.ask(green, 'howdy', 1)
        assert "Got: greetings" == asys1.ask(blue, 'greetings', 1)
        # Verify sub-actors are responsive
        assert 'hey, red' == asys1.ask(blue, (RedActor, 'hey, red'), max_wait)
        assert "howdy howdy" == asys1.ask(green, (BlueActor, 'howdy howdy'),
                                          max_wait)
#        assert "greetings all" == asys1.ask(red, (BlueActor, 'greetings all'), max_wait)
        # Remove color capabilities from two ActorSystems
        asys2.updateCapability('Green')
        asys3.updateCapability('Blue')
        # Verify can no longer create associated Actors
        #    Note: removing Blue from Three should have cause red's
        #    BlueActor child to exit.  If it did, the next assertNone
        #    will pass.
        assert asys1.ask(red, (BlueActor, 'hello'), 1) is None
        assert asys1.ask(red, (GreenActor, 'greetings'), 1) is None
        # Verify can still create Actors where attributes remain
        assert 'go time' == asys1.ask(red, (RedActor, 'go time'), 1)
        # Remove color capabilities from two ActorSystems AGAIN
        asys2.updateCapability('Green')
        asys3.updateCapability('Blue')
        # Verify can no longer create associated Actors
        assert asys1.ask(red, (BlueActor, 'hello'), 1) is None
        assert asys1.ask(red, (GreenActor, 'greetings'), 1) is None
        # Verify can still create Actors where attributes remain
        assert 'go time' == asys1.ask(red, (RedActor, 'go time'), 1)
        # Tell actors to exit
        asys1.tell(red, ActorExitRequest())
        asys1.tell(green, ActorExitRequest())
        asys1.tell(blue, ActorExitRequest())
        time.sleep(0.1)

    # test: removing capability via None value is the same as no value

    def test10_removingColorCapabilitiesOnOtherActorSystemsDoesNotAffectExistingColorActors(self, asys_trio):
        asys1, asys2, asys3 = asys_trio
        actor_system_unsupported(asys1,
                                 "simpleSystemBase",
                                 "multiprocQueueBase")
        # Setup systems
        asys1.updateCapability('Red', True)
        asys2.updateCapability('Green', True)
        asys3.updateCapability('Blue', True)
        max_wait = 0.55
        time.sleep(max_wait*3)  # Allow for propagation (with hysteresis)
        # Create Actors in those systems
        red = asys1.createActor(RedActor)
        green = asys1.createActor(GreenActor)
        blue = asys1.createActor(BlueActor)
        # Remove (non-existent) capabilities from other systems
        asys3.updateCapability('Red', None)
        asys1.updateCapability('Green', None)
        asys2.updateCapability('Blue', None)
        # Verify actors are still responsive
        assert "Got: hello" == asys1.ask(red, 'hello', 1)
        assert "Got: howdy" == asys1.ask(green, 'howdy', 1)
        assert "Got: greetings" == asys1.ask(blue, 'greetings', 1)
        # Tell actors to exit
        asys1.tell(red, ActorExitRequest())
        asys1.tell(green, ActorExitRequest())
        asys1.tell(blue, ActorExitRequest())
        time.sleep(0.1)


    def _actorCount(self, asys1, startAddr):
        s = asys1.ask(startAddr, Thespian_StatusReq(), 1)
        assert isinstance(s, Thespian_ActorStatus)
        return 1 + sum([self._actorCount(asys1, C) for C in s.childActors])

    def test11_allSubActorsNotifiedOfCapabilityChanges(self, asys_trio):
        asys1, asys2, asys3 = asys_trio
        actor_system_unsupported(asys1,
                                 "simpleSystemBase",
                                 "multiprocQueueBase")
        max_wait = 0.5
        # Setup systems
        asys1.updateCapability('Red', True)
        asys1.updateCapability('Green', True)
        asys1.updateCapability('Blue', True)
        asys1.updateCapability('Orange', True)
        # Create Actors in those systems
        red = asys1.createActor(RedActor)
        assert 'long path' == asys1.ask(red, (GreenActor, RedActor,
                                              OrangeActor, BlueActor,
                                              GreenActor,
                                              'long path'),
                                        max_wait)
        assert 6, self._actorCount(asys1 == red)
        # Now remove a capability needed by a deep sub-Actor and
        # verify that sub-Actor (and it's children) are gone.
        asys1.updateCapability('Blue')
        time.sleep(max_wait)
        assert 4 == self._actorCount(asys1, red)


    def test11_1_capabilityRemovalOnlyAffectsOneSystem(self, asys_trio):
        asys1, asys2, asys3 = asys_trio
        actor_system_unsupported(asys1,
                                 "simpleSystemBase",
                                 "multiprocQueueBase")
        # Creates sub-actors in another system.  Removal of a
        # capability on the current environment should not cause
        # impact to sub-actors in another environment
        max_wait = 0.5
        # Setup systems
        asys2.updateCapability('Green', True)
        asys2.updateCapability('Red', True)
        asys2.updateCapability('Blue', True)
        time.sleep(1)  # wait for hysteresis delay of multiple updates
        # Create parent in system one with child in system two
        parent = asys1.createActor(OrangeActor)
        assert "red" == asys1.ask(parent, (RedActor, "red"), 1)
        assert 2 == self._actorCount(asys1, parent)
        # Add capability associated with child in primary system
        asys1.updateCapability('Red', True)
        time.sleep(max_wait)  # allow capabilities to update
        # Remove capability associated with child from primary system;
        # this should not cause the child to exit because it is still
        # in a valid system.
        asys1.updateCapability('Red', None)
        time.sleep(max_wait)  # allow capabilities to update
        assert 2 == self._actorCount(asys1, parent)
        # Removal of the capability in the system hosting the child does cause the child to exit
        asys2.updateCapability('Red', None)
        time.sleep(max_wait)  # allow capabilities to update
        assert 1 == self._actorCount(asys1, parent)


    def test12_updateCapabilitiesAffectsActorDrivenCreateRequests(self, asys_trio):
        asys1, asys2, asys3 = asys_trio
        actor_system_unsupported(asys1,
                                 "simpleSystemBase",
                                 "multiprocQueueBase")
        max_wait = 0.95
        # Setup systems
        asys1.updateCapability('Red', True)

        asys3.updateCapability('Blue', True)
        time.sleep(max_wait)  # Allow for propagation (with hysteresis)
        # Create Actors in those systems
        red = asys1.createActor(RedActor)
        blue = asys1.createActor(BlueActor)
        raises(NoCompatibleSystemForActor, asys1.createActor, GreenActor)
        # Verify a sub-actor cannot create another sub-actor that
        # requires a capability that isn't present (fails to create a
        # GreenActor).
        assert asys1.ask(blue, (RedActor, GreenActor, RedActor, OrangeActor,
                                BlueActor, GreenActor,
                                'long path'),
                         max_wait) is None
        # Now have Red add a couple of capabilities
        assert 'ok' == asys1.ask(red, SetCap('Green', True), 1)
        time.sleep(max_wait)  # allow capabilities to settle
        # Verify that added capability enables a sub-actor to creat new Actors
        assert 'long path' == asys1.ask(blue, (RedActor, GreenActor,
                                               RedActor, OrangeActor,
                                               BlueActor, GreenActor,
                                               'long path'),
                                        max_wait)
        # Remove that capability again
        assert 'ok' == asys1.ask(red, SetCap('Green', None), 1)
        time.sleep(max_wait)  # allow capabilities to settle
        # Now verify that sub-actor cannot create Green actors again
        assert asys1.ask(blue, (RedActor, GreenActor, RedActor,
                                BlueActor, GreenActor,
                                'long path'),
                         max_wait) is None
        # Tell actors to exit
        asys1.tell(red, ActorExitRequest())
        asys1.tell(blue, ActorExitRequest())
        time.sleep(0.1)

    def test13_removeOriginalCapabilitiesAffectsActorDrivenCreateRequests(self, asys_trio):
        asys1, asys2, asys3 = asys_trio
        actor_system_unsupported(asys1,
                                 "simpleSystemBase",
                                 "multiprocQueueBase")
        max_wait = 0.5
        # Setup systems
        asys1.updateCapability('Red', True)
        asys1.updateCapability('Blue', True)
        asys1.updateCapability('Green', True)  # same system
        # Create Actors in those systems
        red = asys1.createActor(RedActor)
        blue = asys1.createActor(BlueActor)
        assert 'long path' == asys1.ask(blue, (GreenActor, RedActor,
                                               BlueActor, GreenActor,
                                               'long path'),
                                        max_wait)
        # Remove an originally-existing capability
        assert 'ok' == asys1.ask(red, SetCap('Green', None), 1)
        time.sleep(max_wait)  # allow capabilities to settle
        # Now verify that sub-actor cannot create Green actors anymore
        assert asys1.ask(blue, (GreenActor, RedActor, BlueActor, GreenActor,
                                'long path'),
                         max_wait) is None
        # Tell actors to exit
        asys1.tell(red, ActorExitRequest())
        asys1.tell(blue, ActorExitRequest())
        time.sleep(0.1)


    # test can create lots of various sub-actors, ensuring the capabilities are plumbed
    # test creation of subactor failure, then add capability, then subactor can be created
    # test creation of subactor, then removal of capability, then recreation elsewhere works

# n.b. no test for MultiprocQueue because it does not support conventions
