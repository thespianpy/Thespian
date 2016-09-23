from pytest import raises
from thespian.test import *
from datetime import timedelta
from thespian.test import ActorSystemTestCase, simpleActorTestLogging
from thespian.actors import *
from thespian.system.messages.status import *
from thespian.system.utilis import fmap


MAX_ASK_DELAY = timedelta(seconds=2)
wait_for_registration = lambda: inTestDelay(timedelta(milliseconds=250))


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


class NotificationHandler(ActorTypeDispatcher):
    def __init__(self, *args, **kw):
        super(NotificationHandler, self).__init__(*args, **kw)
        self.notifications = []

    def receiveMsg_str(self, strmsg, sender):
        self.send(sender, self.notifications)

    def receiveMsg_bool(self, boolmsg, sender):
        self.notifyOnSystemRegistrationChanges(boolmsg)

    def receiveMsg_ActorSystemConventionUpdate(self, update, sender):
        newn = [N for N in self.notifications if N.remoteAdminAddress != update.remoteAdminAddress]
        self.notifications = newn + [update]


def showAdminStatus(actorSys):
    admin = actorSys._systemBase\
                    .transport\
                    .getAddressFromString('127.0.0.1:%d'%actorSys.port_num)
    print('admin address: %s'%str(admin))
    sts = actorSys.ask(admin, Thespian_StatusReq(), 2)
    if sts is None:
        print('no status from system 1')
    else:
        formatStatus(sts, str)


# Convention registration is only supported for multi-process system
# bases that support conventions
unsupported = lambda asys: \
              actor_system_unsupported(asys, 'simpleSystemBase',
                                       'multiprocQueueBase',)



class TestFuncRegistration(object):

    def test_Registration(self, asys, asys2):
        unsupported(asys)
        asys.updateCapability('dog', 'food')
        asys2.updateCapability('barn', 'oats')

        regActor = asys.createActor(PreRegistrationActor)
        raises(NoCompatibleSystemForActor, asys.createActor, Horse)
        raises(NoCompatibleSystemForActor, asys.createActor, Cow)

        rsp = asys.ask(regActor, ("Register", "127.0.0.1:%d"%(asys2.port_num),
                                  {'moo': True}),
                       timedelta(seconds=2))
        wait_for_registration()
        showAdminStatus(asys)
        showAdminStatus(asys2)
        horse = asys.createActor(Horse)
        assert asys.ask(horse, 'bor', 2) == 'Neigh: bor'

        # Also verify that even though the initial preregistration
        # specified a "moo" capability, that the registration process
        # overwrote that with the actual capabilities of asys2 and
        # dropped the moo capability, so it stillshould not be
        # possible to create a Cow Actor.
        raises(NoCompatibleSystemForActor, asys.createActor, Cow)


    def test_RegistrationNotificationWithAddressString(self, asys, asys2):
        unsupported(asys)

        notifications = asys.createActor(NotificationHandler)
        asys.tell(notifications, True)

        asys.updateCapability('dog', 'food')
        asys2.updateCapability('barn', 'oats')

        regActor = asys.createActor(PreRegistrationActor)
        raises(NoCompatibleSystemForActor, asys.createActor, Horse)
        raises(NoCompatibleSystemForActor, asys.createActor, Cow)

        notes = asys.ask(notifications, 'get', MAX_ASK_DELAY)
        assert not notes

        rsp = asys.ask(regActor, ("Register", "127.0.0.1:%d"%(asys2.port_num),
                                  {'moo': True}),
                       MAX_ASK_DELAY)
        wait_for_registration()
        showAdminStatus(asys)
        showAdminStatus(asys2)

        # wait and show should allow actual conventionregistration for
        # full set of remote capabilities.
        notes = asys.ask(notifications, 'get', MAX_ASK_DELAY)
        print('notes:',fmap(str, notes))
        assert len(notes) == 1
        for each in notes:
            assert isinstance(each.remoteAdminAddress, ActorAddress)
            # Actual registration should have actual capabilities
            assert 'barn' in each.remoteCapabilities
            assert each.remoteCapabilities['barn'] == 'oats'
            assert 'Thespian Version' in each.remoteCapabilities
            assert each.remoteAdded

        horse = asys.createActor(Horse)
        assert asys.ask(horse, 'bor', MAX_ASK_DELAY) == 'Neigh: bor'

        # Verify that deregistration can be performed with the remote
        # admin address and that there is a corresponding
        # notification.

        rsp = asys.ask(regActor, ("Deregister",
                                  "127.0.0.1:%d"%(asys2.port_num)),
                                  MAX_ASK_DELAY)

        wait_for_registration()
        showAdminStatus(asys)
        showAdminStatus(asys2)

        notes = asys.ask(notifications, 'get', MAX_ASK_DELAY)
        print(fmap(repr, notes))
        assert len(notes) == 1
        for each in notes:
            print(':: %s @ %s : %s' %
                  (each.remoteAdded,
                   str(each.remoteAdminAddress),
                   str(each.remoteCapabilities)))
            assert isinstance(each.remoteAdminAddress, ActorAddress)
            # Actual registration should have actual capabilities
            assert 'barn' in each.remoteCapabilities
            assert each.remoteCapabilities['barn'] == 'oats'
            assert 'Thespian Version' in each.remoteCapabilities
            # Still a member because it is distinct from the added
            # version
            assert not each.remoteAdded


    def test_RegistrationNotificationWithActorAddress(self, asys, asys2):
        unsupported(asys)

        notifications = asys.createActor(NotificationHandler)
        asys.tell(notifications, True)

        asys.updateCapability('dog', 'food')
        asys2.updateCapability('barn', 'oats')

        regActor = asys.createActor(PreRegistrationActor)
        raises(NoCompatibleSystemForActor, asys.createActor, Horse)
        raises(NoCompatibleSystemForActor, asys.createActor, Cow)

        notes = asys.ask(notifications, 'get', MAX_ASK_DELAY)
        assert not notes

        print('notification address:',str(notifications))
        myaddr = str(notifications).split('|')[1].split(':')[0]
        print('myaddr:',myaddr)

        rsp = asys.ask(regActor, ("Register",
                                  "%s:%d"%(myaddr, asys2.port_num),
                                  {'moo': True}),
                       MAX_ASK_DELAY)
        wait_for_registration()
        showAdminStatus(asys)
        showAdminStatus(asys2)

        # wait and show should allow actual convention registration for
        # full set of remote capabilities, then verify that this has
        # occurred.
        wait_for_registration()  # allow convention registration
        notes = asys.ask(notifications, 'get', MAX_ASK_DELAY)
        print(fmap(str, notes))
        assert len(notes) == 1
        for each in notes:
            print(':: %s @ %s : %s' %
                  (each.remoteAdded,
                   str(each.remoteAdminAddress),
                   str(each.remoteCapabilities)))
            assert isinstance(each.remoteAdminAddress, ActorAddress)
            rmtAdmin = each.remoteAdminAddress
            # Actual registration should have actual capabilities
            assert 'barn' in each.remoteCapabilities
            assert each.remoteCapabilities['barn'] == 'oats'
            assert 'Thespian Version' in each.remoteCapabilities
            assert each.remoteAdded

        horse = asys.createActor(Horse)
        assert asys.ask(horse, 'bor', MAX_ASK_DELAY) == 'Neigh: bor'

        # Verify that deregistration can be performed with the remote
        # admin address and that there is a corresponding
        # notification.

        rsp = asys.ask(regActor, ("Deregister", rmtAdmin), MAX_ASK_DELAY)

        wait_for_registration()
        showAdminStatus(asys)
        showAdminStatus(asys2)

        notes = asys.ask(notifications, 'get', MAX_ASK_DELAY)
        print(fmap(repr, notes))
        assert len(notes) == 1
        for each in notes:
            print(':: %s @ %s : %s' %
                  (each.remoteAdded,
                   str(each.remoteAdminAddress),
                   str(each.remoteCapabilities)))
            assert isinstance(each.remoteAdminAddress, ActorAddress)
            # Actual registration should have actual capabilities
            assert 'barn' in each.remoteCapabilities
            assert each.remoteCapabilities['barn'] == 'oats'
            assert 'Thespian Version' in each.remoteCapabilities
            assert not each.remoteAdded


    def testBadRegistrationAddress(self, asys, asys2):
        unsupported(asys)
        asys.updateCapability('dog', 'food')
        asys2.updateCapability('barn', 'oats')

        regActor = asys.createActor(PreRegistrationActor)
        rsp = asys.ask(regActor, ("RegisterNOT", "10.101.10:9999",
                                           {'cat': 'meow'}),
                                timedelta(seconds=2))
        assert rsp == "OK"


    def test_deRegistration(self, asys, asys2):
        unsupported(asys)
        asys.updateCapability('dog', 'food')
        asys2.updateCapability('barn', 'oats')

        regActor = asys.createActor(PreRegistrationActor)
        raises(NoCompatibleSystemForActor, asys.createActor, Horse)

        rsp = asys.ask(regActor, ("Register", "127.0.0.1:%d"%asys2.port_num,
                                  {'barn': 'oats'}),
                       timedelta(seconds=2))
        assert rsp == "OK"
        wait_for_registration()

        horse = asys.createActor(Horse)
        assert asys.ask(horse, 'bor', 2) == 'Neigh: bor'
        assert asys2.ask(horse, 'sound', 2) == 'Neigh: sound'

        rsp = asys.ask(regActor, ("Deregister", "127.0.0.1:%d"%asys2.port_num),
                       timedelta(seconds=2))
        assert rsp == "OK"
        wait_for_registration()
        showAdminStatus(asys)
        showAdminStatus(asys2)

        # Unfortunately, this is harder than expected, because often
        # there ends up being a dual-registration: one with the
        # loopback address and one with the "public" address.
        # Therefore, cannot do:
        #    raises(NoCompatibleSystemForActor, self.asys1.createActor, Horse)
        # See the test_publicAddrDeRegistration test for a workaround

        assert asys2.ask(horse, 'still running', 2) == 'Neigh: still running'


    def test_publicAddrDeRegistration(self, asys, asys2):
        unsupported(asys)
        asys.updateCapability('dog', 'food')
        asys2.updateCapability('barn', 'oats')

        regActor = asys.createActor(PreRegistrationActor)
        regActorIP = str(regActor).split('|')[1].split(':')[0]  # brittle
        raises(NoCompatibleSystemForActor, asys.createActor, Horse)

        rsp = asys.ask(regActor, ("Register", regActorIP+":%d"%asys2.port_num,
                                  {'barn': 'oats'}),
                       timedelta(seconds=2))
        assert rsp == "OK"
        wait_for_registration()

        horse = asys.createActor(Horse)
        assert asys.ask(horse, 'bor', 2) == 'Neigh: bor'
        assert asys2.ask(horse, 'sound', 2) == 'Neigh: sound'

        showAdminStatus(asys)
        rsp = asys.ask(regActor, ("Deregister", regActorIP+":%d"%asys.port_num),
                       timedelta(seconds=2))
        assert rsp == "OK"

        # Preregistration is cancelled, but remote system is still normally registered.
        assert asys2.ask(horse, 'still running', 2) == 'Neigh: still running'

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
        assert True
