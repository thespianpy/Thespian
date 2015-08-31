"""Various tests that determine whether updating capabilities for
multiple ActorSystems in a Convention are working correctly.  These
tests run somewhat slowly because they must allow time for
coordination of effects an hysteresis of same between the multiple
systems (which should not be an issue under normal operations).
"""

import unittest
from thespian.test import ActorSystemTestCase
import time
import thespian.test.helpers
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


class BaseCapabilityUpdates(object):
    def setUp(self):
        self.systems = {}

    def startSystems(self, portOffset):
        # Only define base capabilities, not extended capabilities
        self.capabilities = { 'One': { 'Admin Port': 19001 + portOffset + self.basePortOffset, },
                              'Two': { 'Admin Port': 19002 + portOffset + self.basePortOffset,
                                       'Convention Address.IPv4': ('', 19001 + portOffset + self.basePortOffset), },
                              'Three': { 'Admin Port': 19003 + portOffset + self.basePortOffset,
                                         'Convention Address.IPv4': ('', 19001 + portOffset + self.basePortOffset), },
                          }
        for each in ['One', 'Two', 'Three']:  # 'One' must be first
            self.systems[each] = ActorSystem(self.actorSystemBase, self.capabilities[each],
                                             logDefs = ActorSystemTestCase.getDefaultTestLogging())
        time.sleep(0.25)  # Wait for Actor Systems to start

    def tearDown(self):
        for each in self.systems:
            self.systems[each].shutdown()

    def test00_systemsRunnable(self):
        self.startSystems(0)

    def test01_defaultSystemsDoNotSupportColorActors(self):
        self.startSystems(10)
        self.assertRaises(NoCompatibleSystemForActor, self.systems['One'].createActor, RedActor)
        self.assertRaises(NoCompatibleSystemForActor, self.systems['One'].createActor, BlueActor)
        self.assertRaises(NoCompatibleSystemForActor, self.systems['One'].createActor, GreenActor)

    def test02_addColorCapabilitiesAllowsColorActors(self):
        self.startSystems(20)
        # Setup Systems
        self.systems['One'].updateCapability('Red', True)
        self.systems['Two'].updateCapability('Green', True)
        self.systems['Three'].updateCapability('Blue', True)
        reasonableActorResponseTime = 0.2
        time.sleep(reasonableActorResponseTime*3)  # Allow for propagation (with hysteresis)
        # Create one actor in each system
        red = self.systems['One'].createActor(RedActor)
        green = self.systems['One'].createActor(GreenActor)
        blue = self.systems['One'].createActor(BlueActor)
        # Verify got valid ActorAddresses
        self.assertIsNotNone(red)
        self.assertIsNotNone(green)
        self.assertIsNotNone(blue)
        self.assertIsInstance(red, ActorAddress)
        self.assertIsInstance(green, ActorAddress)
        self.assertIsInstance(blue, ActorAddress)
        # Verify actors are responsive
        self.assertEqual("Got: hello", self.systems['One'].ask(red, 'hello', 1))
        self.assertEqual("Got: howdy", self.systems['One'].ask(green, 'howdy', 1))
        self.assertEqual("Got: greetings", self.systems['One'].ask(blue, 'greetings', 1))
        # Tell actors to exit
        self.systems['One'].tell(red, ActorExitRequest())
        self.systems['One'].tell(green, ActorExitRequest())
        self.systems['One'].tell(blue, ActorExitRequest())
        time.sleep(0.1)

    def test02_1_addColorCapabilitiesAllowsColorActorsAndSubActors(self):
        self.startSystems(30)
        # Setup Systems
        self.systems['One'].updateCapability('Red', True)
        self.systems['Two'].updateCapability('Green', True)
        self.systems['Three'].updateCapability('Blue', True)
        reasonableActorResponseTime = 0.2
        time.sleep(reasonableActorResponseTime*3)  # Allow for propagation (with hysteresis)
        # Create one actor in each system
        red = self.systems['One'].createActor(RedActor)
        green = self.systems['One'].createActor(GreenActor)
        blue = self.systems['One'].createActor(BlueActor)
        orange = self.systems['One'].createActor(OrangeActor)
        # Verify got valid ActorAddresses
        self.assertIsNotNone(red)
        self.assertIsNotNone(green)
        self.assertIsNotNone(blue)
        self.assertIsNotNone(orange)
        self.assertIsInstance(red, ActorAddress)
        self.assertIsInstance(green, ActorAddress)
        self.assertIsInstance(blue, ActorAddress)
        self.assertIsInstance(orange, ActorAddress)
        # Verify actors are responsive
        self.assertEqual("Got: hello", self.systems['One'].ask(red, 'hello', 1))
        self.assertEqual("Got: howdy", self.systems['One'].ask(green, 'howdy', 1))
        self.assertEqual("Got: greetings", self.systems['One'].ask(blue, 'greetings', 1))
        self.assertEqual("Got: aloha", self.systems['One'].ask(orange, 'aloha', 1))
        # Create a chain of multiple colors from each top level
        self.assertEqual("path1", self.systems['One'].ask(red, (BlueActor, GreenActor, RedActor, GreenActor, BlueActor, RedActor, "path1"), 1))
        self.assertEqual("path2", self.systems['One'].ask(green, (BlueActor, GreenActor, RedActor, GreenActor, BlueActor, RedActor, "path2"), 1))
        self.assertEqual("path3", self.systems['One'].ask(blue, (BlueActor, GreenActor, RedActor, GreenActor, OrangeActor, BlueActor, RedActor, "path3"), 1))
        # Tell actors to exit
        self.systems['One'].tell(red, ActorExitRequest())
        self.systems['One'].tell(green, ActorExitRequest())
        self.systems['One'].tell(blue, ActorExitRequest())
        time.sleep(0.1)

    def test03_addMultipleColorCapabilitiesToOneActorSystemAllowsColorActors(self):
        self.startSystems(40)
        # Setup systems
        self.systems['One'].updateCapability('Red', True)
        self.systems['Two'].updateCapability('Green', True)
        self.systems['Two'].updateCapability('Blue', True)
        reasonableActorResponseTime = 0.2
        time.sleep(reasonableActorResponseTime*6)  # Allow for propagation (with hysteresis)
        # Create Actors (two in system Two)
        red = self.systems['One'].createActor(RedActor)
        green = self.systems['One'].createActor(GreenActor)
        blue = self.systems['One'].createActor(BlueActor)
        # Verify got valid ActorAddresses
        self.assertIsNotNone(red)
        self.assertIsNotNone(green)
        self.assertIsNotNone(blue)
        self.assertIsInstance(red, ActorAddress)
        self.assertIsInstance(green, ActorAddress)
        self.assertIsInstance(blue, ActorAddress)
        # Verify actors are responsive
        self.assertEqual("Got: hello", self.systems['One'].ask(red, 'hello', 1))
        self.assertEqual("Got: howdy", self.systems['One'].ask(green, 'howdy', 1))
        self.assertEqual("Got: greetings", self.systems['One'].ask(blue, 'greetings', 1))
        # Tell actors to exit
        self.systems['One'].tell(red, ActorExitRequest())
        self.systems['One'].tell(green, ActorExitRequest())
        self.systems['One'].tell(blue, ActorExitRequest())
        time.sleep(0.1)

    def test04_addMultipleColorCapabilitiesToLeaderActorSystemAllowsColorActors(self):
        self.startSystems(50)
        # Setup systems
        self.systems['One'].updateCapability('Red', True)
        self.systems['One'].updateCapability('Green', True)
        self.systems['One'].updateCapability('Blue', True)
        reasonableActorResponseTime = 0.2
        time.sleep(reasonableActorResponseTime*6)  # Allow for propagation (with hysteresis)
        # Create Actors (all in system One)
        red = self.systems['One'].createActor(RedActor)
        green = self.systems['One'].createActor('thespian.test.testUpdateSystemCapabilities.GreenActor')
        blue = self.systems['One'].createActor(BlueActor)
        # Verify got valid ActorAddresses
        self.assertIsNotNone(red)
        self.assertIsNotNone(green)
        self.assertIsNotNone(blue)
        self.assertIsInstance(red, ActorAddress)
        self.assertIsInstance(green, ActorAddress)
        self.assertIsInstance(blue, ActorAddress)
        # Verify actors are responsive
        self.assertEqual("Got: hello", self.systems['One'].ask(red, 'hello', 1))
        self.assertEqual("Got: howdy", self.systems['One'].ask(green, 'howdy', 1))
        self.assertEqual("Got: greetings", self.systems['One'].ask(blue, 'greetings', 1))
        # Tell actors to exit
        self.systems['One'].tell(red, ActorExitRequest())
        self.systems['One'].tell(green, ActorExitRequest())
        self.systems['One'].tell(blue, ActorExitRequest())
        time.sleep(0.1)

    def test04_1_actorAddCapabilitiesEnablesOtherActors(self):
        self.startSystems(60)
        # Setup system (only one needed, because an Actor can only
        # modify its own system)
        self.systems['One'].updateCapability('Red', True)
        # Create Actors (all in system One)
        red = self.systems['One'].createActor(RedActor)
        self.assertRaises(NoCompatibleSystemForActor, self.systems['One'].createActor, BlueActor)
        self.assertRaises(NoCompatibleSystemForActor, self.systems['One'].createActor, GreenActor)
        orange = self.systems['One'].createActor(OrangeActor)
        # Verify actors are responsive
        self.assertEqual("Got: Hello", self.systems['One'].ask(red, 'Hello', 1))
        self.assertEqual("Got: Aloha", self.systems['One'].ask(orange, 'Aloha', 1))
        # Now have Red add a couple of capabilities
        self.systems['One'].tell(red, SetCap('Green', True))
        self.systems['One'].tell(red, SetCap('Blue', True))
        time.sleep(0.1)  # allow actor to process these messages
        # And create some Actors needing those capabilities
        green = self.systems['One'].createActor(GreenActor)
        blue = self.systems['One'].createActor(BlueActor)
        self.assertIsNotNone(green)
        self.assertIsNotNone(blue)
        self.assertIsInstance(red, ActorAddress)
        self.assertIsInstance(green, ActorAddress)
        self.assertIsInstance(blue, ActorAddress)
        self.assertEqual("Got: hello", self.systems['One'].ask(red, 'hello', 1))
        self.assertEqual("Got: howdy", self.systems['One'].ask(green, 'howdy', 1))
        self.assertEqual("Got: greetings", self.systems['One'].ask(blue, 'greetings', 1))
        self.assertEqual("Got: Aloha", self.systems['One'].ask(orange, 'Aloha', 1))
        # Tell actors to exit
        self.systems['One'].tell(red, ActorExitRequest())
        self.systems['One'].tell(green, ActorExitRequest())
        self.systems['One'].tell(blue, ActorExitRequest())
        time.sleep(0.1)

    def test05_removingColorCapabilitiesKillsExistingColorActors(self):
        self.startSystems(70)
        # Setup systems
        self.systems['One'].updateCapability('Red', True)
        self.systems['Two'].updateCapability('Green', True)
        self.systems['Three'].updateCapability('Blue', True)
        reasonableActorResponseTime = 0.2
        time.sleep(reasonableActorResponseTime*3)  # Allow for propagation (with hysteresis)
        # Create Actors
        red = self.systems['One'].createActor(RedActor)
        green = self.systems['One'].createActor(GreenActor)
        blue = self.systems['One'].createActor(BlueActor)
        orange = self.systems['One'].createActor(OrangeActor)
        # Verify got valid ActorAddresses
        self.assertIsNotNone(red)
        self.assertIsNotNone(green)
        self.assertIsNotNone(blue)
        self.assertIsNotNone(orange)
        self.assertIsInstance(red, ActorAddress)
        self.assertIsInstance(green, ActorAddress)
        self.assertIsInstance(blue, ActorAddress)
        self.assertIsInstance(orange, ActorAddress)
        # Verify actors are responsive
        self.assertEqual("Got: hello", self.systems['One'].ask(red, 'hello', 1))
        self.assertEqual("Got: howdy", self.systems['One'].ask(green, 'howdy', 1))
        self.assertEqual("Got: greetings", self.systems['One'].ask(blue, 'greetings', 1))
        self.assertEqual("Got: aloha", self.systems['One'].ask(orange, 'aloha', 1))
        # Remove color capabilities from ActorSystems
        self.systems['One'].updateCapability('Red', None)
        self.systems['Two'].updateCapability('Green', None)
        self.systems['Three'].updateCapability('Blue', None)
        time.sleep(0.2)  # processing time allowance
        # Verify all Actors are no longer present.
        self.assertIsNone(self.systems['One'].ask(red, '1', 1))
        self.assertIsNone(self.systems['One'].ask(green, '2', 1))
        self.assertIsNone(self.systems['One'].ask(blue, '3', 1))
        self.assertEqual("Got: aloha", self.systems['One'].ask(orange, 'aloha', 1))
        time.sleep(0.1)

    def test05_1_removingColorCapabilitiesViaActorKillsExistingColorActors(self):
        self.startSystems(80)
        # Setup systems
        self.systems['One'].updateCapability('Red', True)
        self.systems['Two'].updateCapability('Green', True)
        self.systems['Three'].updateCapability('Blue', True)
        reasonableActorResponseTime = 0.2
        time.sleep(reasonableActorResponseTime*3)  # Allow for propagation (with hysteresis)
        # Create Actors
        red = self.systems['One'].createActor(RedActor)
        green = self.systems['One'].createActor(GreenActor)
        blue = self.systems['One'].createActor(BlueActor)
        orange = self.systems['One'].createActor(OrangeActor)
        # Verify got valid ActorAddresses
        self.assertIsNotNone(red)
        self.assertIsNotNone(green)
        self.assertIsNotNone(blue)
        self.assertIsNotNone(orange)
        self.assertIsInstance(red, ActorAddress)
        self.assertIsInstance(green, ActorAddress)
        self.assertIsInstance(blue, ActorAddress)
        self.assertIsInstance(orange, ActorAddress)
        # Verify actors are responsive
        self.assertEqual("Got: hello", self.systems['One'].ask(red, 'hello', 1))
        self.assertEqual("Got: howdy", self.systems['One'].ask(green, 'howdy', 1))
        self.assertEqual("Got: greetings", self.systems['One'].ask(blue, 'greetings', 1))
        self.assertEqual("Got: aloha", self.systems['One'].ask(orange, 'aloha', 1))
        # Remove color capabilities from ActorSystems
        self.systems['One'].tell(red, SetCap('Red', False))
        self.systems['One'].tell(blue, SetCap('Blue', False))
        time.sleep(0.4)  # allow actor to process these messages
        # Verify affected Actors are no longer present.
        self.assertIsNone(self.systems['One'].ask(red, '1', 1))
        self.assertEqual("Got: Howdy", self.systems['One'].ask(green, 'Howdy', 1))
        self.assertIsNone(self.systems['One'].ask(blue, '3', 1))
        self.assertEqual("Got: aloha", self.systems['One'].ask(orange, 'aloha', 1))
        # Tell actors to exit
        self.systems['One'].tell(green, ActorExitRequest())
        self.systems['One'].tell(orange, ActorExitRequest())
        time.sleep(0.1)

    def test06_removingColorCapabilitiesPreventsNewColorActors(self):
        self.startSystems(90)
        # Setup systems
        self.systems['One'].updateCapability('Red', True)
        self.systems['Two'].updateCapability('Green', True)
        self.systems['Three'].updateCapability('Blue', True)
        reasonableActorResponseTime = 0.2
        time.sleep(reasonableActorResponseTime*6)  # Allow for propagation (with hysteresis)
        # Create Actors
        red = self.systems['One'].createActor(RedActor)
        green = self.systems['One'].createActor(GreenActor)
        blue = self.systems['One'].createActor(BlueActor)
        # Verify got valid ActorAddresses
        self.assertIsNotNone(red)
        self.assertIsNotNone(green)
        self.assertIsNotNone(blue)
        self.assertIsInstance(red, ActorAddress)
        self.assertIsInstance(green, ActorAddress)
        self.assertIsInstance(blue, ActorAddress)
        # Verify actors are responsive
        self.assertEqual("Got: hello", self.systems['One'].ask(red, 'hello', reasonableActorResponseTime))
        self.assertEqual("Got: howdy", self.systems['One'].ask(green, 'howdy', reasonableActorResponseTime))
        self.assertEqual("Got: greetings", self.systems['One'].ask(blue, 'greetings', reasonableActorResponseTime))
        # Remove one Capability and verify that all Actors created via that ActorSystem are removed

        self.systems['Three'].updateCapability('Blue', None)
        time.sleep(0.1)
        self.assertIsNone(self.systems['One'].ask(blue, 'yono', reasonableActorResponseTime))
        self.assertEqual("Got: hellono", self.systems['One'].ask(red, 'hellono', reasonableActorResponseTime))
        self.assertEqual("Got: hino", self.systems['One'].ask(green, 'hino', reasonableActorResponseTime))

        self.systems['One'].updateCapability('Red', None)
        time.sleep(0.1)  # wait for capability update to propagate
        self.assertIsNone(self.systems['One'].ask(red, 'hello', reasonableActorResponseTime))
        self.assertEqual('Got: hi', self.systems['One'].ask(green, 'hi', reasonableActorResponseTime))
        self.assertIsNone(self.systems['One'].ask(blue, 'yo', reasonableActorResponseTime))
        # Verify no Actors requiring the removed capabilities can be
        # created, but other kinds can still be created.
        self.assertRaises(NoCompatibleSystemForActor, self.systems['One'].createActor, RedActor)
        red = None
        green = self.systems['One'].createActor(GreenActor)
        self.assertRaises(NoCompatibleSystemForActor,
                          self.systems['One'].createActor, BlueActor)
        # Add back the Blue capability and verify the Actor can now be created
        self.systems['Three'].updateCapability('Blue', True)
        time.sleep(0.25)
        blue = self.systems['One'].createActor(BlueActor)
        self.assertIsNone(red)
        self.assertIsNotNone(green)
        self.assertIsNotNone(blue)
        self.assertIsInstance(green, ActorAddress)
        self.assertIsInstance(blue, ActorAddress)
        self.assertEqual("Got: howdy howdy", self.systems['One'].ask(green, 'howdy howdy', reasonableActorResponseTime))
        self.assertEqual("Got: greetings all", self.systems['One'].ask(blue, 'greetings all', reasonableActorResponseTime))
        self.assertIsNone(self.systems['One'].ask(blue, (RedActor, 'hey, red'), reasonableActorResponseTime))
        self.assertEqual("hey, blue", self.systems['One'].ask(green, (BlueActor, 'hey, blue'), reasonableActorResponseTime*10))
        self.assertEqual("hey, green", self.systems['One'].ask(blue, (GreenActor, 'hey, green'), reasonableActorResponseTime*10))
        # Remove remaining capabilities
        self.systems['Two'].updateCapability('Green', None)
        self.systems['One'].tell(blue, SetCap('Blue', None))
        time.sleep(0.1)  # allow actor to process these messages
        # No new actors can be created for any color
        self.assertRaises(NoCompatibleSystemForActor, self.systems['One'].createActor, RedActor)
        self.assertRaises(NoCompatibleSystemForActor, self.systems['One'].createActor, BlueActor)
        self.assertRaises(NoCompatibleSystemForActor, self.systems['One'].createActor, GreenActor)

    def test07_removingNonExistentCapabilitiesHasNoEffect(self):
        self.startSystems(100)
        reasonableActorResponseTime = 0.65
        # Setup systems
        self.systems['One'].updateCapability('Red', True)
        self.systems['Two'].updateCapability('Green', True)
        self.systems['Three'].updateCapability('Blue', True)
        time.sleep(reasonableActorResponseTime*2)  # Allow for propagation (with hysteresis)
        # Create Actors
        red = self.systems['One'].createActor(RedActor)
        green = self.systems['One'].createActor(GreenActor)
        blue = self.systems['One'].createActor(BlueActor)
        # Verify got valid ActorAddresses
        self.assertIsNotNone(red)
        self.assertIsNotNone(green)
        self.assertIsNotNone(blue)
        self.assertIsInstance(red, ActorAddress)
        self.assertIsInstance(green, ActorAddress)
        self.assertIsInstance(blue, ActorAddress)
        # Verify actors are responsive
        self.assertEqual("Got: hello", self.systems['One'].ask(red, 'hello', 1))
        self.assertEqual("Got: howdy", self.systems['One'].ask(green, 'howdy', 1))
        self.assertEqual("Got: greetings", self.systems['One'].ask(blue, 'greetings', 1))
        self.assertEqual('long path', self.systems['One'].ask(blue, (RedActor, GreenActor, RedActor, BlueActor, GreenActor, 'long path'),
                                                              reasonableActorResponseTime))
        # Verify sub-actors are responsive
        self.assertEqual('bluered', self.systems['One'].ask(blue, (RedActor, 'bluered'), reasonableActorResponseTime))
        self.assertEqual("greenblue", self.systems['One'].ask(green, (BlueActor, 'greenblue'), reasonableActorResponseTime))
        self.assertEqual("bluegreen", self.systems['One'].ask(blue, (GreenActor, 'bluegreen'), reasonableActorResponseTime))
        # Remove non-color capabilities from ActorSystems
        self.systems['One'].updateCapability('Frog', None)
        self.systems['One'].tell(blue, SetCap('Bark', None))
        self.systems['One'].updateCapability('Cow', None)
        time.sleep(0.1)
        # Verify actors are still responsive
        self.assertEqual("Got: hello", self.systems['One'].ask(red, 'hello', 1))
        self.assertEqual("Got: howdy", self.systems['One'].ask(green, 'howdy', 1))
        self.assertEqual("Got: greetings", self.systems['One'].ask(blue, 'greetings', 1))
        # Verify sub-actors are still responsive
        self.assertEqual('hey, red', self.systems['One'].ask(blue, (RedActor, 'hey, red'), reasonableActorResponseTime))
        self.assertEqual("howdy howdy", self.systems['One'].ask(green, (BlueActor, 'howdy howdy'), reasonableActorResponseTime))
        self.assertEqual("greetings all", self.systems['One'].ask(red, (GreenActor, 'greetings all'), reasonableActorResponseTime))
        # Verify new sub-actors can be created
        self.assertEqual('long path', self.systems['One'].ask(blue, (RedActor, GreenActor, RedActor, BlueActor, GreenActor, 'long path'),
                                                              reasonableActorResponseTime))
        # Tell actors to exit
        self.systems['One'].tell(red, ActorExitRequest())
        self.systems['One'].tell(green, ActorExitRequest())
        self.systems['One'].tell(blue, ActorExitRequest())
        time.sleep(0.1)

    def test08_settingCapabilityToSameValueHasNoEffect(self):
        self.startSystems(110)
        reasonableActorResponseTime = 0.5
        # Setup systems
        self.systems['One'].updateCapability('Red', True)
        self.systems['Two'].updateCapability('Green', True)
        self.systems['Three'].updateCapability('Blue', True)
        time.sleep(reasonableActorResponseTime*1.8)  # Allow for propagation (with hysteresis)
        # Create Actors
        red = self.systems['One'].createActor(RedActor)
        green = self.systems['One'].createActor(GreenActor)
        blue = self.systems['One'].createActor(BlueActor)
        # Verify got valid ActorAddresses
        self.assertIsNotNone(red)
        self.assertIsNotNone(green)
        self.assertIsNotNone(blue)
        self.assertIsInstance(red, ActorAddress)
        self.assertIsInstance(green, ActorAddress)
        self.assertIsInstance(blue, ActorAddress)
        # Verify actors are responsive
        self.assertEqual("Got: hello", self.systems['One'].ask(red, 'hello', 1))
        self.assertEqual("Got: howdy", self.systems['One'].ask(green, 'howdy', 1))
        self.assertEqual("Got: greetings", self.systems['One'].ask(blue, 'greetings', 1))
        # Verify sub-actors are responsive
        self.assertEqual('hey, red', self.systems['One'].ask(blue, (RedActor, 'hey, red'), reasonableActorResponseTime))
        self.assertEqual("howdy howdy", self.systems['One'].ask(green, (GreenActor, 'howdy howdy'), reasonableActorResponseTime))
        self.assertEqual("greetings all", self.systems['One'].ask(red, (BlueActor, 'greetings all'), reasonableActorResponseTime))
        # Remove non-color capabilities from ActorSystems
        self.systems['One'].updateCapability('Red', True)
        self.systems['Two'].updateCapability('Green', True)
        self.systems['One'].tell(blue, SetCap('Blue', True))
        # Verify actors are still responsive
        self.assertEqual("Got: hello", self.systems['One'].ask(red, 'hello', 1))
        self.assertEqual("Got: howdy", self.systems['One'].ask(green, 'howdy', 1))
        self.assertEqual("Got: greetings", self.systems['One'].ask(blue, 'greetings', 1))
        # Verify sub-actors are still responsive
        self.assertEqual('hey, red', self.systems['One'].ask(blue, (RedActor, 'hey, red'), reasonableActorResponseTime))
        self.assertEqual("howdy howdy", self.systems['One'].ask(green, (RedActor, 'howdy howdy'), reasonableActorResponseTime))
        self.assertEqual("greetings all", self.systems['One'].ask(red, (BlueActor, 'greetings all'), reasonableActorResponseTime))
        # Verify new sub-actors can be created
        self.assertEqual('long path', self.systems['One'].ask(blue, (RedActor, GreenActor, RedActor, BlueActor, GreenActor, 'long path'),
                                                              reasonableActorResponseTime))
        # Tell actors to exit
        self.systems['One'].tell(red, ActorExitRequest())
        self.systems['One'].tell(green, ActorExitRequest())
        self.systems['One'].tell(blue, ActorExitRequest())
        time.sleep(0.1)


    def test09_removingCapabilityTwiceHasNoEffectTheSecondTime(self):
        self.startSystems(120)
        reasonableActorResponseTime = 0.2
        # Setup systems
        self.systems['One'].updateCapability('Red', True)
        self.systems['Two'].updateCapability('Green', True)
        self.systems['Three'].updateCapability('Blue', True)
        time.sleep(reasonableActorResponseTime*6)  # Allow for propagation (with hysteresis)
        # Create Actors
        red = self.systems['One'].createActor(RedActor)
        green = self.systems['One'].createActor('thespian.test.testUpdateSystemCapabilities.GreenActor')
        blue = self.systems['One'].createActor(BlueActor)
        # Verify got valid ActorAddresses
        self.assertIsNotNone(red)
        self.assertIsNotNone(green)
        self.assertIsNotNone(blue)
        self.assertIsInstance(red, ActorAddress)
        self.assertIsInstance(green, ActorAddress)
        self.assertIsInstance(blue, ActorAddress)
        # Verify actors are responsive
        self.assertEqual("Got: hello", self.systems['One'].ask(red, 'hello', 1))
        self.assertEqual("Got: howdy", self.systems['One'].ask(green, 'howdy', 1))
        self.assertEqual("Got: greetings", self.systems['One'].ask(blue, 'greetings', 1))
        # Verify sub-actors are responsive
        self.assertEqual('hey, red', self.systems['One'].ask(blue, (RedActor, 'hey, red'), reasonableActorResponseTime))
        self.assertEqual("howdy howdy", self.systems['One'].ask(green, (BlueActor, 'howdy howdy'), reasonableActorResponseTime))
#        self.assertEqual("greetings all", self.systems['One'].ask(red, (BlueActor, 'greetings all'), reasonableActorResponseTime))
        # Remove color capabilities from two ActorSystems
        self.systems['Two'].updateCapability('Green')
        self.systems['Three'].updateCapability('Blue')
        # Verify can no longer create associated Actors
        #    Note: removing Blue from Three should have cause red's
        #    BlueActor child to exit.  If it did, the next assertNone
        #    will pass.
        self.assertIsNone(self.systems['One'].ask(red, (BlueActor, 'hello'), 1))
        self.assertIsNone(self.systems['One'].ask(red, (GreenActor, 'greetings'), 1))
        # Verify can still create Actors where attributes remain
        self.assertEqual('go time', self.systems['One'].ask(red, (RedActor, 'go time'), 1))
        # Remove color capabilities from two ActorSystems AGAIN
        self.systems['Two'].updateCapability('Green')
        self.systems['Three'].updateCapability('Blue')
        # Verify can no longer create associated Actors
        self.assertIsNone(self.systems['One'].ask(red, (BlueActor, 'hello'), 1))
        self.assertIsNone(self.systems['One'].ask(red, (GreenActor, 'greetings'), 1))
        # Verify can still create Actors where attributes remain
        self.assertEqual('go time', self.systems['One'].ask(red, (RedActor, 'go time'), 1))
        # Tell actors to exit
        self.systems['One'].tell(red, ActorExitRequest())
        self.systems['One'].tell(green, ActorExitRequest())
        self.systems['One'].tell(blue, ActorExitRequest())
        time.sleep(0.1)

    # test: removing capability via None value is the same as no value

    def test10_removingColorCapabilitiesOnOtherActorSystemsDoesNotAffectExistingColorActors(self):
        self.startSystems(130)
        # Setup systems
        self.systems['One'].updateCapability('Red', True)
        self.systems['Two'].updateCapability('Green', True)
        self.systems['Three'].updateCapability('Blue', True)
        reasonableActorResponseTime = 0.55
        time.sleep(reasonableActorResponseTime*3)  # Allow for propagation (with hysteresis)
        # Create Actors in those systems
        red = self.systems['One'].createActor(RedActor)
        green = self.systems['One'].createActor(GreenActor)
        blue = self.systems['One'].createActor(BlueActor)
        # Remove (non-existent) capabilities from other systems
        self.systems['Three'].updateCapability('Red', None)
        self.systems['One'].updateCapability('Green', None)
        self.systems['Two'].updateCapability('Blue', None)
        # Verify actors are still responsive
        self.assertEqual("Got: hello", self.systems['One'].ask(red, 'hello', 1))
        self.assertEqual("Got: howdy", self.systems['One'].ask(green, 'howdy', 1))
        self.assertEqual("Got: greetings", self.systems['One'].ask(blue, 'greetings', 1))
        # Tell actors to exit
        self.systems['One'].tell(red, ActorExitRequest())
        self.systems['One'].tell(green, ActorExitRequest())
        self.systems['One'].tell(blue, ActorExitRequest())
        time.sleep(0.1)


    def _actorCount(self, startAddr):
        s = self.systems['One'].ask(startAddr, Thespian_StatusReq(), 1)
        self.assertIsInstance(s, Thespian_ActorStatus)
        return 1 + sum([self._actorCount(C) for C in s.childActors])

    def test11_allSubActorsNotifiedOfCapabilityChanges(self):
        self.startSystems(140)
        reasonableActorResponseTime = 0.5
        # Setup systems
        self.systems['One'].updateCapability('Red', True)
        self.systems['One'].updateCapability('Green', True)
        self.systems['One'].updateCapability('Blue', True)
        self.systems['One'].updateCapability('Orange', True)
        # Create Actors in those systems
        red = self.systems['One'].createActor(RedActor)
        self.assertEqual('long path', self.systems['One'].ask(red, (GreenActor, RedActor, OrangeActor, BlueActor, GreenActor, 'long path'),
                                                              reasonableActorResponseTime))
        self.assertEqual(6, self._actorCount(red))
        # Now remove a capability needed by a deep sub-Actor and
        # verify that sub-Actor (and it's children) are gone.
        self.systems['One'].updateCapability('Blue')
        time.sleep(reasonableActorResponseTime)
        self.assertEqual(4, self._actorCount(red))


    def test12_updateCapabilitiesAffectsActorDrivenCreateRequests(self):
        self.startSystems(150)
        reasonableActorResponseTime = 0.65
        # Setup systems
        self.systems['One'].updateCapability('Red', True)

        self.systems['Three'].updateCapability('Blue', True)
        time.sleep(reasonableActorResponseTime)  # Allow for propagation (with hysteresis)
        # Create Actors in those systems
        red = self.systems['One'].createActor(RedActor)
        blue = self.systems['One'].createActor(BlueActor)
        self.assertRaises(NoCompatibleSystemForActor, self.systems['One'].createActor, GreenActor)
        # Verify a sub-actor cannot create another sub-actor that
        # requires a capability that isn't present (fails to create a
        # GreenActor).
        self.assertIsNone(self.systems['One'].ask(blue, (RedActor, GreenActor, RedActor, OrangeActor, BlueActor, GreenActor, 'long path'),
                                                              reasonableActorResponseTime))
        # Now have Red add a couple of capabilities
        self.systems['One'].tell(red, SetCap('Green', True))
        time.sleep(reasonableActorResponseTime)  # allow capabilities to settle
        # Verify that added capability enables a sub-actor to creat new Actors
        self.assertEqual('long path', self.systems['One'].ask(blue, (RedActor, GreenActor, RedActor, OrangeActor, BlueActor, GreenActor, 'long path'),
                                                              reasonableActorResponseTime))
        # Remove that capability again
        self.systems['One'].tell(red, SetCap('Green', None))
        time.sleep(reasonableActorResponseTime)  # allow capabilities to settle
        # Now verify that sub-actor cannot create Green actors again
        self.assertIsNone(self.systems['One'].ask(blue, (RedActor, GreenActor, RedActor, BlueActor, GreenActor, 'long path'),
                                                              reasonableActorResponseTime))
        # Tell actors to exit
        self.systems['One'].tell(red, ActorExitRequest())
        self.systems['One'].tell(blue, ActorExitRequest())
        time.sleep(0.1)

    def test13_removeOriginalCapabilitiesAffectsActorDrivenCreateRequests(self):
        self.startSystems(160)
        reasonableActorResponseTime = 0.5
        # Setup systems
        self.systems['One'].updateCapability('Red', True)
        self.systems['One'].updateCapability('Blue', True)
        self.systems['One'].updateCapability('Green', True)  # same system
        # Create Actors in those systems
        red = self.systems['One'].createActor(RedActor)
        blue = self.systems['One'].createActor(BlueActor)
        self.assertEqual('long path', self.systems['One'].ask(blue, (GreenActor, RedActor, BlueActor, GreenActor, 'long path'),
                                                              reasonableActorResponseTime))
        # Remove an originally-existing capability
        self.systems['One'].tell(red, SetCap('Green', None))
        time.sleep(reasonableActorResponseTime)  # allow capabilities to settle
        # Now verify that sub-actor cannot create Green actors anymore
        self.assertIsNone(self.systems['One'].ask(blue, (GreenActor, RedActor, BlueActor, GreenActor, 'long path'),
                                                              reasonableActorResponseTime))
        # Tell actors to exit
        self.systems['One'].tell(red, ActorExitRequest())
        self.systems['One'].tell(blue, ActorExitRequest())
        time.sleep(0.1)


    # test can create lots of various sub-actors, ensuring the capabilities are plumbed
    # test creation of subactor failure, then add capability, then subactor can be created
    # test creation of subactor, then removal of capability, then recreation elsewhere works


class TestMultiprocTCPCapabilityUpdates(BaseCapabilityUpdates, unittest.TestCase):
    testbase='MultiprocTCP'
    scope='func'
    actorSystemBase = 'multiprocTCPBase'
    basePortOffset = 0


class TestMultiprocUDPCapabilityUpdates(BaseCapabilityUpdates, unittest.TestCase):
    testbase='MultiprocUDP'
    scope='func'
    actorSystemBase = 'multiprocUDPBase'
    basePortOffset = 500

# n.b. no test for MultiprocQueue because it does not support conventions
