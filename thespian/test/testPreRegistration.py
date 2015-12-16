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
        # asys1 is the main Actor System the tests will interact with
        self.asys1 = ActorSystem('multiprocTCPBase',
                                 capabilities={'dog':'food',
                                               'Admin Port': 11192},
                                 logDefs = simpleActorTestLogging(),
                                 transientUnique = True)
        # asys2 is a different Actor System that is initially
        # independent, but can support a Horse Actor.
        self.asys2 = ActorSystem('multiprocTCPBase',
                                 capabilities={'barn': 'oats',
                                               'Admin Port': 11193},
                                 logDefs = simpleActorTestLogging(),
                                 transientUnique = True)

    def tearDown(self):
        self.asys1.shutdown()
        self.asys2.shutdown()

    def test_Registration(self):
        regActor = self.asys1.createActor(PreRegistrationActor)
        self.assertRaises(NoCompatibleSystemForActor, self.asys1.createActor, Horse)
        self.assertRaises(NoCompatibleSystemForActor, self.asys1.createActor, Cow)

        rsp = self.asys1.ask(regActor, ("Register", "127.0.0.1:11193",
                                        {'moo': True}),
                             timedelta(seconds=2))
        showAdminStatus(self.asys1, '127.0.0.1:11192')
        showAdminStatus(self.asys2, '127.0.0.1:11193')
        horse = self.asys1.createActor(Horse)
        self.assertEqual(self.asys1.ask(horse, 'bor', 2), 'Neigh: bor')

        # Also verify that even though the initial preregistration
        # specified a "moo" capability, that the registration process
        # overwrote that with the actual capabilities of asys2 and
        # dropped the moo capability, so it stillshould not be
        # possible to create a Cow Actor.
        self.assertRaises(NoCompatibleSystemForActor, self.asys1.createActor, Cow)


    def testBadRegistrationAddress(self):
        regActor = ActorSystem().createActor(PreRegistrationActor)
        rsp = ActorSystem().ask(regActor, ("RegisterNOT", "10.101.10:9999",
                                           {'cat': 'meow'}),
                                timedelta(seconds=2))
        self.assertEqual(rsp, "OK")

    def test_deRegistration(self):
        regActor = self.asys1.createActor(PreRegistrationActor)
        self.assertRaises(NoCompatibleSystemForActor, self.asys1.createActor, Horse)

        rsp = self.asys1.ask(regActor, ("Register", "127.0.0.1:11193",
                                        {'barn': 'oats'}),
                             timedelta(seconds=2))
        self.assertEqual(rsp, "OK")

        horse = self.asys1.createActor(Horse)
        self.assertEqual(self.asys1.ask(horse, 'bor', 2), 'Neigh: bor')
        self.assertEqual(self.asys2.ask(horse, 'sound', 2), 'Neigh: sound')

        rsp = self.asys1.ask(regActor, ("Deregister", "127.0.0.1:11193"),
                             timedelta(seconds=2))
        self.assertEqual(rsp, "OK")
        showAdminStatus(self.asys1, '127.0.0.1:11192')
        showAdminStatus(self.asys2, '127.0.0.1:11193')

        # Unfortunately, this is harder than expected, because often
        # there ends up being a dual-registration: one with the
        # loopback address and one with the "public" address.
        # Therefore, cannot do:
        #    self.assertRaises(NoCompatibleSystemForActor, self.asys1.createActor, Horse)
        # See the test_publicAddrDeRegistration test for a workaround

        self.assertEqual(self.asys2.ask(horse, 'still running', 2), 'Neigh: still running')

    def test_publicAddrDeRegistration(self):
        regActor = self.asys1.createActor(PreRegistrationActor)
        regActorIP = str(regActor).split('|')[1].split(':')[0]  # brittle
        self.assertRaises(NoCompatibleSystemForActor, self.asys1.createActor, Horse)

        rsp = self.asys1.ask(regActor, ("Register", regActorIP+":11193",
                                        {'barn': 'oats'}),
                             timedelta(seconds=2))
        self.assertEqual(rsp, "OK")

        horse = self.asys1.createActor(Horse)
        self.assertEqual(self.asys1.ask(horse, 'bor', 2), 'Neigh: bor')
        self.assertEqual(self.asys2.ask(horse, 'sound', 2), 'Neigh: sound')

        rsp = self.asys1.ask(regActor, ("Deregister", regActorIP+":11193"),
                             timedelta(seconds=2))
        self.assertEqual(rsp, "OK")
        showAdminStatus(self.asys1, regActorIP+':11192')
        showAdminStatus(self.asys2, regActorIP+':11193')

        self.assertRaises(NoCompatibleSystemForActor, self.asys1.createActor, Horse)
        self.assertEqual(self.asys2.ask(horse, 'still running', 2), 'Neigh: still running')

