import unittest
from datetime import timedelta
import thespian.test.helpers
from thespian.test import ActorSystemTestCase, simpleActorTestLogging
from thespian.actors import *
from thespian.system.messages.status import *


class PreRegistrationActor(ActorTypeDispatcher):

    def receiveMsg_tuple(self, msg, sender):
        if msg[0] == "Register":
            self.preRegisterRemoteSystem(msg[1], msg[2])
        if msg[0] == "Deregister":
            self.deRegisterRemoteSystem(msg[1])
        self.send(sender, "OK")


@requireCapability('barn', 'oats')
class Horse(ActorTypeDispatcher):
    def receiveMsg_str(self, strmsg, sender):
        self.send(sender, 'Neigh: ' + strmsg)


@requireCapability('moo')
class Cow(ActorTypeDispatcher):
    def receiveMsg_str(self, strmsg, sender):
        self.send(sender, 'Moo: ' + strmsg)



def showAdminStatus(actorSys, admin_address):
    admin = actorSys._systemBase.transport.getAddressFromString(admin_address)
    print('admin address: %s'%str(admin))
    sts = actorSys.ask(admin, Thespian_StatusReq(), 2)
    if sts is None:
        print('no status from system 1')
    else:
        formatStatus(sts, str)


class TestRegistration(unittest.TestCase):
    testbase = 'MultiprocTCP'
    scope = 'func'

    def setUp(self):
        self.asys1 = None
        self.asys2 = None
        self.baseport = 0

    def startup(self):
        # asys1 is the main Actor System the tests will interact with
        self.asys1 = ActorSystem('multiprocTCPBase',
                                 capabilities={'dog':'food',
                                               'Admin Port': 11192 + self.baseport},
                                 logDefs = simpleActorTestLogging(),
                                 transientUnique = True)
        # asys2 is a different Actor System that is initially
        # independent, but can support a Horse Actor.
        self.asys2 = ActorSystem('multiprocTCPBase',
                                 capabilities={'barn': 'oats',
                                               'Admin Port': 11193 + self.baseport},
                                 logDefs = simpleActorTestLogging(),
                                 transientUnique = True)


    def tearDown(self):
        # n.b. shutdown the second actor system first:
        #   1. Some tests ask asys1 to create an actor
        #   2. That actor is actually supported by asys2
        #   3. There is an external port the tester uses for each asys
        #   4. When asys2 is shutdown, it will attempt to notify the
        #      parent of the actor that the actor is dead
        #   5. This parent is the external port for asys1.
        #   6. If asys1 is shutdown first, then asys2 must time out
        #      on the transmit attempt (usually 5 minutes) before
        #      it can exit.
        #   7. If the test is re-run within this 5 minute period, it will fail
        #      because the old asys2 is still existing but in shutdown state
        #      (and will therefore rightfully refuse new actions).
        # By shutting down asys2 first, the parent notification can be
        # performed and subsequent runs don't encounter the lingering
        # asys2.
        if self.asys2: self.asys2.shutdown()
        if self.asys1: self.asys1.shutdown()

    def test_Registration(self):
        self.startup()
        regActor = self.asys1.createActor(PreRegistrationActor)
        self.assertRaises(NoCompatibleSystemForActor, self.asys1.createActor, Horse)
        self.assertRaises(NoCompatibleSystemForActor, self.asys1.createActor, Cow)

        rsp = self.asys1.ask(regActor, ("Register", "127.0.0.1:%d"%(11193+self.baseport),
                                        {'moo': True}),
                             timedelta(seconds=2))
        showAdminStatus(self.asys1, '127.0.0.1:%d'%(11192+self.baseport))
        showAdminStatus(self.asys2, '127.0.0.1:%d'%(11193+self.baseport))
        horse = self.asys1.createActor(Horse)
        self.assertEqual(self.asys1.ask(horse, 'bor', 2), 'Neigh: bor')

        # Also verify that even though the initial preregistration
        # specified a "moo" capability, that the registration process
        # overwrote that with the actual capabilities of asys2 and
        # dropped the moo capability, so it stillshould not be
        # possible to create a Cow Actor.
        self.assertRaises(NoCompatibleSystemForActor, self.asys1.createActor, Cow)


    def testBadRegistrationAddress(self):
        self.baseport = 3
        self.startup()
        regActor = ActorSystem().createActor(PreRegistrationActor)
        rsp = ActorSystem().ask(regActor, ("RegisterNOT", "10.101.10:9999",
                                           {'cat': 'meow'}),
                                timedelta(seconds=2))
        self.assertEqual(rsp, "OK")

    def test_deRegistration(self):
        self.baseport = 6
        self.startup()
        regActor = self.asys1.createActor(PreRegistrationActor)
        self.assertRaises(NoCompatibleSystemForActor, self.asys1.createActor, Horse)

        rsp = self.asys1.ask(regActor, ("Register", "127.0.0.1:%d"%(11193+self.baseport),
                                        {'barn': 'oats'}),
                             timedelta(seconds=2))
        self.assertEqual(rsp, "OK")

        horse = self.asys1.createActor(Horse)
        self.assertEqual(self.asys1.ask(horse, 'bor', 2), 'Neigh: bor')
        self.assertEqual(self.asys2.ask(horse, 'sound', 2), 'Neigh: sound')

        rsp = self.asys1.ask(regActor, ("Deregister", "127.0.0.1:%d"%(11193+self.baseport)),
                             timedelta(seconds=2))
        self.assertEqual(rsp, "OK")
        showAdminStatus(self.asys1, '127.0.0.1:%d'%(11192+self.baseport))
        showAdminStatus(self.asys2, '127.0.0.1:%d'%(11193+self.baseport))

        # Unfortunately, this is harder than expected, because often
        # there ends up being a dual-registration: one with the
        # loopback address and one with the "public" address.
        # Therefore, cannot do:
        #    self.assertRaises(NoCompatibleSystemForActor, self.asys1.createActor, Horse)
        # See the test_publicAddrDeRegistration test for a workaround

        self.assertEqual(self.asys2.ask(horse, 'still running', 2), 'Neigh: still running')

    def test_publicAddrDeRegistration(self):
        self.baseport = 9
        self.startup()
        regActor = self.asys1.createActor(PreRegistrationActor)
        regActorIP = str(regActor).split('|')[1].split(':')[0]  # brittle
        self.assertRaises(NoCompatibleSystemForActor, self.asys1.createActor, Horse)

        rsp = self.asys1.ask(regActor, ("Register", regActorIP+":%d"%(11193+self.baseport),
                                        {'barn': 'oats'}),
                             timedelta(seconds=2))
        self.assertEqual(rsp, "OK")

        horse = self.asys1.createActor(Horse)
        self.assertEqual(self.asys1.ask(horse, 'bor', 2), 'Neigh: bor')
        self.assertEqual(self.asys2.ask(horse, 'sound', 2), 'Neigh: sound')

        showAdminStatus(self.asys1, regActorIP+':%d'%(11192+self.baseport))
        rsp = self.asys1.ask(regActor, ("Deregister", regActorIP+":%d"%(11193+self.baseport)),
                             timedelta(seconds=2))
        self.assertEqual(rsp, "OK")

        # Preregistration is cancelled, but remote system is still normally registered.
        self.assertEqual(self.asys2.ask(horse, 'still running', 2), 'Neigh: still running')

        # No easy way to verify that the deregistration occurred.  The
        # asys2 registration with asys1 will remain valid with asys1
        # until it misses the pre-determined number of
        # checkins... even if asys2 is restarted.

        # Also note that since the Horse actor is running in System 2
        # but was registered in System 1, if the shutdown is sent to
        # system 2 it will linger attempting to deliver the Horse
        # ChildActorExited message to system 1, interfering with the
        # subsequent attempt to restart system 2.  Therefore, Horse
        # must be stopped via the system1 endpoint before system 2 is
        # restarted.

        # At this point, just assert that the test reached this point
        # without failure.
        self.assertTrue(True)
