from thespian.test import *
from time import sleep
import logging
from thespian.actors import *
from datetime import timedelta


class PreRegActor(ActorTypeDispatcher):
    def receiveMsg_str(self, regaddr, sender):
        self.preRegisterRemoteSystem(regaddr, {})
        self.send(sender, 'Registered')


@pytest.fixture
def testsystems(request, asys):
    asys.updateCapability('Jedi Council', True)
    dagobah = similar_asys(asys, in_convention=not asys.txonly,
                           start_wait=False,
                           capabilities = { 'Swamp': True })
    hoth = similar_asys(asys, in_convention=not asys.txonly,
                        start_wait=False,
                        capabilities = { 'Snow': True })
    endor = similar_asys(asys, in_convention=not asys.txonly,
                         start_wait=False,
                         capabilities = { 'Trees': True })
    naboo = similar_asys(asys, in_convention=not asys.txonly,
                         start_wait=True,  # only need to wait for the last one
                         capabilities = { 'Ocean': True })
    request.addfinalizer(lambda dagobah=dagobah, hoth=hoth, endor=endor,
                         naboo=naboo:
                         dagobah.shutdown() or
                         hoth.shutdown() or
                         endor.shutdown() or
                         naboo.shutdown())
    if asys.txonly:
        assert 'Registered' == asys.ask(asys.createActor(PreRegActor),
                                        'localhost:%d'%dagobah.port_num,
                                        timedelta(seconds=3))
        assert 'Registered' == asys.ask(asys.createActor(PreRegActor),
                                        'localhost:%d'%hoth.port_num,
                                        timedelta(seconds=3))
        assert 'Registered' == asys.ask(asys.createActor(PreRegActor),
                                        'localhost:%d'%endor.port_num,
                                        timedelta(seconds=3))
        assert 'Registered' == asys.ask(asys.createActor(PreRegActor),
                                        'localhost:%d'%naboo.port_num,
                                        timedelta(seconds=3))
        sleep(0.25)  # allow System2 to join the Convention
    return asys, dagobah, hoth, endor, naboo


class Conversation:
    def __init__(self, seed, addressList):
        self.addressList = addressList
        self.seed = seed
        self.roundTrip = False
        self.results = []

class WhoAreYou: pass


class Yoda(Actor):
    @staticmethod
    def actorSystemCapabilityCheck(capabilities, actorRequirements):
        return capabilities.get('Swamp', False)
    def __init__(self):
        self.numTrained = 0
        self.numFailed = 0
    def receiveMessage(self, msg, sender):
        if isinstance(msg, Conversation):
            if not msg.roundTrip:
                msg.addressList.insert(0, sender)
                msg.roundTrip = True
            next = msg.addressList.pop()
            msg.results.append('Use the Force, you must, to ' + msg.seed)
            self.send(next, msg)
        elif isinstance(msg, WhoAreYou):
            self.send(sender, self.myAddress)
        elif msg == 'Padawan':
            self.createActor(Luke)
            self.numTrained += 1
        elif msg == 'Obi Wan':
            self.createActor(ObiWan)
            self.numTrained += 1
        elif msg == 'Training Completed?':
            self.send(sender, (self.numTrained, self.numFailed))
        elif isinstance(msg, ChildActorExited):
            self.numFailed += 1
        elif type(msg) == type(""):
            self.send(sender, 'Use the Force, you must, to ' + msg)


class Luke(Actor):
    @staticmethod
    def actorSystemCapabilityCheck(capabilities, actorRequirements):
        return False  # Luke has no patience, that one
    def receiveMessage(self, msg, sender):
        raise RuntimeError('Should never be called!')


class ObiWan(Actor):
    @staticmethod
    def actorSystemCapabilityCheck(capabilities, actorRequirements):
        return capabilities.get('Trees', False)

    def receiveMessage(self, msg, sender):
        raise RuntimeError('Should never be called!')


class Tauntaun(Actor):
    @staticmethod
    def actorSystemCapabilityCheck(capabilities, actorRequirements):
        return capabilities.get('Snow', False)
    def receiveMessage(self, msg, sender):
        if type(msg) == type(""):
            self.send(sender, 'waagh! ' + msg)
        elif isinstance(msg, Conversation):
            if not msg.roundTrip:
                msg.addressList.insert(0, sender)
                msg.roundTrip = True
            next = msg.addressList.pop()
            msg.results.append('waagh! ' + msg.seed)
            self.send(next, msg)
        elif isinstance(msg, WhoAreYou):
            self.send(sender, self.myAddress)


class Tell(object):
    def __init__(self, requester, request_msg):
        self.requester = requester
        self.request_msg = request_msg

class Ewok(Actor):
    @staticmethod
    def actorSystemCapabilityCheck(capabilities, actorRequirements):
        return capabilities.get('Trees', False)
    def receiveMessage(self, msg, sender):
        if type(msg) == type(""):
            if msg == "Greet JarJar":
                jarjar = self.createActor(JarJar, targetActorRequirements={'WantsToSee': 'Trees'})
                self.send(jarjar, Tell(sender, 'Trees'))
            elif msg == 'Freeze JarJar':
                self.send(self.createActor(JarJar,
                                           targetActorRequirements={'WantsToSee': 'Snow',
                                                                    'Has': 'A fur coat?'}),
                          Tell(sender, 'Snow'))
            elif msg == "Greet Guest":
                guest = self.createActor(Guest, targetActorRequirements={'Invited to': 'Trees'})
                self.send(guest, Tell(sender, 'Trees'))
            elif msg == 'Freeze JarJar':
                self.send(self.createActor(JarJar,
                                           targetActorRequirements={'WantsToSee': 'Snow',
                                                                    'Has': 'A fur coat?'}),
                          Tell(sender, 'Snow'))
            else:
                self.send(sender, 'We cook ' + msg + ' for dinner')
        elif isinstance(msg, Conversation):
            if not msg.roundTrip:
                msg.addressList.insert(0, sender)
                msg.roundTrip = True
            next = msg.addressList.pop()
            msg.results.append('We cook ' + msg.seed + ' for dinner')
            self.send(next, msg)
        elif isinstance(msg, WhoAreYou):
            self.send(sender, self.myAddress)


class JarJar(Actor):
    @staticmethod
    def actorSystemCapabilityCheck(capabilities, actorRequirements):
        return capabilities.get('Ocean', False) or \
            capabilities.get(actorRequirements.get('WantsToSee', 'None'), False)
    def receiveMessage(self, msg, sender):
        if type(msg) == type(""):
            self.send(sender, msg + '?  How rude!')
        elif isinstance(msg, Tell):
            self.send(msg.requester, msg.request_msg + '?  Huh?')
        elif isinstance(msg, Conversation):
            if not msg.roundTrip:
                msg.addressList.insert(0, sender)
                msg.roundTrip = True
            next = msg.addressList.pop()
            msg.results.append(msg.seed + '?  How rude!')
            self.send(next, msg)
        elif isinstance(msg, WhoAreYou):
            self.send(sender, self.myAddress)


class Guest(Actor):
    # This actor MUST be passed an actorRequirements containing 'Invited to' or it will fail
    @staticmethod
    def actorSystemCapabilityCheck(capabilities, actorRequirements):
        return capabilities.get(actorRequirements['Invited to'], False)
    def receiveMessage(self, msg, sender):
        if type(msg) == type(""):
            self.send(sender, "Thank you for " + msg)
        elif isinstance(msg, Tell):
            self.send(msg.requester, 'Thanks for the ' + msg.request_msg)


class TestFuncManyActorSystem(object):

    def test00_primeLogging(self, testsystems):
        asys, dagobah, hoth, endor, naboo = testsystems
        pass


    def test01_checkAllSystemStart(self, testsystems):
        asys, dagobah, hoth, endor, naboo = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        pass


    def test02_checkAllSystemAndActorsStart(self, testsystems):
        asys, dagobah, hoth, endor, naboo = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        ewok     = asys.createActor(Ewok)
        jarjar   = asys.createActor(JarJar)
        tauntaun = asys.createActor(Tauntaun)
        yoda     = asys.createActor(Yoda)
        sleep(1)  # wait for things to settle

    def test03_checkAllSystemsAndActorsStart(self, testsystems):
        asys, dagobah, hoth, endor, naboo = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        ewok     = asys.createActor(Ewok)
        jarjar   = asys.createActor(JarJar)
        tauntaun = asys.createActor(Tauntaun)
        yoda     = asys.createActor(Yoda)
        r = asys.ask(ewok, 'hi', 0.25)
        assert r == 'We cook hi for dinner'
        r = asys.ask(yoda, 'hi', 0.25)
        assert r, 'Use the Force, you must == to hi'
        r = asys.ask(jarjar, 'hi', 0.25)
        assert r == 'hi?  How rude!'
        r = asys.ask(tauntaun, 'hi', 0.25)
        assert r == 'waagh! hi'

    def test04_conversation(self, testsystems):
        asys, dagobah, hoth, endor, naboo = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        ewok     = asys.createActor(Ewok)
        jarjar   = asys.createActor(JarJar)
        tauntaun = asys.createActor(Tauntaun)
        yoda     = asys.createActor(Yoda)
        minutes = asys.ask(ewok,
                           Conversation('hi', [yoda, jarjar, tauntaun]), 0.25)
        assert minutes is not None
        assert minutes.results == ['We cook hi for dinner',
                                   'waagh! hi',
                                   'hi?  How rude!',
                                   'Use the Force, you must, to hi',
        ]

        minutes2 = asys.ask(yoda,
                            Conversation('hi', [yoda, jarjar, jarjar, ewok,
                                                tauntaun, ewok, tauntaun]),
                            0.25)
        assert minutes2 is not None
        assert minutes2.results == ['Use the Force, you must, to hi',
                                    'waagh! hi',
                                    'We cook hi for dinner',
                                    'waagh! hi',
                                    'We cook hi for dinner',
                                    'hi?  How rude!',
                                    'hi?  How rude!',
                                    'Use the Force, you must, to hi',
        ]

    def test05_uniqueAddresses(self, testsystems):
        asys, dagobah, hoth, endor, naboo = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        ewok     = asys.createActor(Ewok)
        jarjar   = asys.createActor(JarJar)
        tauntaun = asys.createActor(Tauntaun)
        yoda     = asys.createActor(Yoda)

        r = asys.ask(ewok, WhoAreYou(), 0.25)
        assert r is not None
        r = asys.ask(jarjar, WhoAreYou(), 0.25)
        assert r is not None
        r = asys.ask(tauntaun, WhoAreYou(), 0.25)
        assert r is not None
        r = asys.ask(yoda, WhoAreYou(), 0.25)
        assert r is not None

        r1 = asys.ask(ewok, WhoAreYou(), 0.25)
        r2 = asys.ask(jarjar, WhoAreYou(), 0.25)
        assert r1 != r2
        r1 = asys.ask(ewok, WhoAreYou(), 0.25)
        r2 = asys.ask(tauntaun, WhoAreYou(), 0.25)
        assert r1 != r2
        r1 = asys.ask(ewok, WhoAreYou(), 0.25)
        r2 = asys.ask(yoda, WhoAreYou(), 0.25)
        assert r1 != r2
        r1 = asys.ask(jarjar, WhoAreYou(), 0.25)
        r2 = asys.ask(tauntaun, WhoAreYou(), 0.25)
        assert r1 != r2
        r1 = asys.ask(jarjar, WhoAreYou(), 0.25)
        r2 = asys.ask(yoda, WhoAreYou(), 0.25)
        assert r1 != r2
        r1 = asys.ask(tauntaun, WhoAreYou(), 0.25)
        r2 = asys.ask(yoda, WhoAreYou(), 0.25)
        assert r1 != r2


    def test06_primaryActorRequirements(self, testsystems):
        asys, dagobah, hoth, endor, naboo = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        jarjar = asys.createActor(JarJar)  # on Naboo
        r = asys.ask(jarjar, 'hi', 0.25)
        assert r == 'hi?  How rude!'

        naboo.shutdown()
        sleep(0.15)

        try:
            r = asys.ask(jarjar, 'hi again', 0.25)
            assert r == None # JarJar is gone
        except ActorSystemFailure:
            # Not all ActorSystems fail this way, but most will
            pass

        # Create a new JarJar on Endor
        jarjar = asys.createActor(
            JarJar,
            targetActorRequirements={'WantsToSee': 'Trees',
                                     'TravellingWith': 'ObiWan'})
        r = asys.ask(jarjar, 'hiya', 0.25)
        assert r == 'hiya?  How rude!'

        endor.shutdown()
        sleep(0.15)

        try:
            r = asys.ask(jarjar, 'hi ho', 0.25)
            assert r == None # JarJar is gone
        except ActorSystemFailure:
            # Not all ActorSystems fail this way, but most will
            pass

        # Now specify requirements no system can meet: JarJar is not created
        try:
            jarjar = asys.createActor(JarJar,
                                      targetActorRequirements={'WantsToSee': 'Trees'})
            assert jarjar == None
        except NoCompatibleSystemForActor:
            # This is a valid, alternate behavior for some ActorSystems
            pass

        # Now bring JarJar to Hoth
        jarjar = asys.createActor(JarJar,
                                  targetActorRequirements={'WantsToSee': 'Snow'})
        r = asys.ask(jarjar, 'hi you', 0.25)
        assert r == 'hi you?  How rude!'


    def test07_subActorRequirements(self, testsystems):
        # JarJar's home world is taken by the Empire
        asys, dagobah, hoth, endor, naboo = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        naboo.shutdown()
        sleep(0.15)

        # JarJar can't go home anymore
        try:
            jarjar = asys.createActor(JarJar)
            assert jarjar == None
        except NoCompatibleSystemForActor:
            # This is a valid, alternate behavior for some ActorSystems
            pass

        ewok = asys.createActor(Ewok)
        r = asys.ask(ewok, 'beans', 0.25)
        assert r == 'We cook beans for dinner'

        # Ewok creates a JarJar actor in the same ActorSystem (Endor)
        r = asys.ask(ewok, 'Greet JarJar', 0.35)
        assert r == 'Trees?  Huh?'

        # Ewok creats a JarJar actor but requires it to be in the Hoth ActorSystem
        if 'TXOnly' in asys.base_name:
            pytest.xfail('In pre-registered convention, endor cannot currently see leader or hoth')
        r = asys.ask(ewok, 'Freeze JarJar', 1.35)
        assert r == 'Snow?  Huh?'


    def test08_requiredRequirementsNotPassedFails(self, testsystems):
        asys, dagobah, hoth, endor, naboo = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        try:
            g1 = asys.createActor(Guest)
            assert g1 == None
        except NoCompatibleSystemForActor:
            # Confirmed: Guest must be invited
            pass

    def test09_requiredRequirementsPassedToTopLevelSucceeds(self, testsystems):
        asys, dagobah, hoth, endor, naboo = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        g1 = asys.createActor(Guest,
                              targetActorRequirements={'Invited to': 'Ocean'})
        r = asys.ask(g1, 'the seashell', 0.25)
        assert r == 'Thank you for the seashell'

    def test10_requiredRequirementsPassedViaSubLevelSucceeds(self, testsystems):
        asys, dagobah, hoth, endor, naboo = testsystems
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        ewok = asys.createActor(Ewok)
        r = asys.ask(ewok, 'beans', 0.25)
        assert r == 'We cook beans for dinner'

        # Ewok creates a Guest actor in the same ActorSystem (Endor)
        r = asys.ask(ewok, 'Greet Guest', 0.25)
        assert r == 'Thanks for the Trees'



# ----------------------------------------------------------------------

class Notified(Actor):
    def receiveMessage(self, msg, sender):
        logging.info('Notified got: %s', str(msg))
        if msg == 'register':
            self.notifications = []
            self.notifyOnSystemRegistrationChanges(True)
        elif isinstance(msg, ActorSystemConventionUpdate):
            self.notifications.append(msg)
        elif msg == 'notifications':
            self.send(sender, self.notifications)


class TestFuncConventionWatcher(object):

    def testNotifications(self, asys):
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        if 'TXOnly' in asys.base_name:
            pytest.xfail('In pre-registered convention, convention registrations are not functional at present')

        asys.updateCapability('Jedi Council', True)

        watcher = asys.createActor(Notified)
        asys.tell(watcher, 'register')
        sleep(0.10)  # wait for watcher to register

        # Now start each of the secondary ActorSystems; their
        # registration should be noted by the Actor registered for
        # such notifications.

        dagobah = similar_asys(asys, in_convention=not asys.txonly,
                               start_wait=False,
                               capabilities = { 'Swamp': True })
        hoth = similar_asys(asys, in_convention=not asys.txonly,
                            start_wait=False,
                            capabilities = { 'Snow': True })
        endor = similar_asys(asys, in_convention=not asys.txonly,
                             start_wait=False,
                             capabilities = { 'Trees': True })
        naboo = similar_asys(asys, in_convention=not asys.txonly,
                             start_wait=True,  # only need to wait for the last one
                             capabilities = { 'Ocean': True })

        try:
            # Verify all anticipated registrations actually occurred.

            for X in range(300):
                registrations = asys.ask(watcher, 'notifications', 1)
                print(registrations)
                if 4 == len(registrations):
                    break
                sleep(0.01)  # wait for more registrations to complete
            assert 4 == len(registrations)
            for each in registrations:
                assert each.remoteAdded
                assert isinstance(each.remoteAdminAddress, ActorAddress)
                assert isinstance(each.remoteCapabilities, dict)

            # Now ask an ActorSystem to exit

            hoth.shutdown()
            hoth = None

            # Verify that the convention deregistration occurred

            for X in range(30):
                registrations2 = asys.ask(watcher, 'notifications', 1)
                if 5 == len(registrations2):
                    break
                sleep(0.01)  # wait for Hoth system to exit and deregister
            assert 5 == len(registrations2)
            for each in registrations:
                assert isinstance(each.remoteAdminAddress, ActorAddress)
                assert isinstance(each.remoteCapabilities, dict)
            assert 4 == sum([{True:1, False:0}[R.remoteAdded]
                             for R in registrations])

        finally:
            if dagobah: dagobah.shutdown()
            if hoth: hoth.shutdown()
            if endor: endor.shutdown()
            if naboo: naboo.shutdown()


    def testNotificationsUponRegistration(self, asys):
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        if 'TXOnly' in asys.base_name:
            pytest.xfail('In pre-registered convention, convention registrations are not functional at present')

        asys.updateCapability('Jedi Council', True)

        watcher = asys.createActor(Notified)

        # Now start each of the secondary ActorSystems; their
        # registration should be noted by the Actor registered for
        # such notifications.

        dagobah = similar_asys(asys, in_convention=not asys.txonly,
                               start_wait=False,
                               capabilities = { 'Swamp': True })
        hoth = similar_asys(asys, in_convention=not asys.txonly,
                            start_wait=False,
                            capabilities = { 'Snow': True })
        endor = similar_asys(asys, in_convention=not asys.txonly,
                             start_wait=False,
                             capabilities = { 'Trees': True })
        naboo = similar_asys(asys, in_convention=not asys.txonly,
                             start_wait=True,  # only need to wait for the last one
                             capabilities = { 'Ocean': True })
        sleep(0.10)  # wait for systems to start

        asys.tell(watcher, 'register')
        sleep(0.10)  # wait for watcher to register

        try:
            # Verify all anticipated registrations actually occurred.

            for X in range(300):
                registrations = asys.ask(watcher, 'notifications', 1)
                print(registrations)
                if 4 == len(registrations):
                    break
                sleep(0.01)  # wait for more registrations to complete
            assert 4 == len(registrations)
            for each in registrations:
                assert each.remoteAdded
                assert isinstance(each.remoteAdminAddress, ActorAddress)
                assert isinstance(each.remoteCapabilities, dict)

            # Now ask an ActorSystem to exit

            hoth.shutdown()
            hoth = None

            # Verify that the convention deregistration occurred

            for X in range(30):
                registrations2 = asys.ask(watcher, 'notifications', 1)
                if 5 == len(registrations2):
                    break
                sleep(0.01)  # wait for Hoth system to exit and deregister
            assert 5 == len(registrations2)
            for each in registrations:
                assert isinstance(each.remoteAdminAddress, ActorAddress)
                assert isinstance(each.remoteCapabilities, dict)
            assert 4 == sum([{True:1, False:0}[R.remoteAdded]
                             for R in registrations])

        finally:
            if dagobah: dagobah.shutdown()
            if hoth: hoth.shutdown()
            if endor: endor.shutdown()
            if naboo: naboo.shutdown()


class TestFuncConventionDeregistration(object):

    def testNotifications(self, asys):

        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        if 'TXOnly' in asys.base_name:
            pytest.xfail('In pre-registered convention, convention registrations are not functional at present')

        asys.updateCapability('Jedi Council', True)

        watcher = asys.createActor(Notified)
        asys.tell(watcher, 'register')
        sleep(0.2)  # wait for watcher to register

        # Now start each of the secondary ActorSystems; their
        # registration should be noted by the Actor registered for
        # such notifications.

        dagobah = similar_asys(asys, in_convention=not asys.txonly,
                               start_wait=False,
                               capabilities = { 'Swamp': True })
        endor = similar_asys(asys, in_convention=not asys.txonly,
                             start_wait=False,
                             capabilities = { 'Trees': True })

        try:

            # Verify all anticipated registrations actually occurred.

            for X in range(50):
                registrations = asys.ask(watcher, 'notifications', 1)
                print(registrations)
                if 2 == len(registrations):
                    break
                sleep(0.01)    # wait for systems to startup and register
            assert 2 == len(registrations)
            for each in registrations:
                assert each.remoteAdded
                assert isinstance(each.remoteAdminAddress, ActorAddress)
                assert isinstance(each.remoteCapabilities, dict)

            # Now there are 3 actor Systems:
            #    Jedi Council (convention leader)
            #    Endor (Trees)
            #    Dagobah (Swamp)
            # Create some Actors:
            #    Yoda (from Primary, created in system Dagobah)
            #       ObiWan  (from Yoda, through Jedi Council to system Endor)
            #       Luke    (from Yoda, but cannot start this anywhere)
            # Verify that ObiWan starts and stays started, but that Luke "starts" and subsequently exits.

            yoda = asys.createActor(Yoda)
            r = asys.ask(yoda, 'train', 2)
            assert 'Use the Force, you must, to train' == r
            r = asys.ask(yoda, 'Training Completed?', 2)
            assert  (0,0) == r
            asys.tell(yoda, 'Obi Wan')
            asys.tell(yoda, 'Padawan')
            sleep(0.25)  # allow time for Yoda to fail training a young Padawan
            r = asys.ask(yoda, 'Training Completed?', 2)
            assert  (2,1) == r

            # Now ask an ActorSystem to exit.  This is the ActorSystem
            # where Obi Wan is, so that will cause Obi Wan to go away
            # as well.

            endor.shutdown()
            endor = None

            # KWQ: how to get Endor to abruptly exit without shutting
            # down ObiWan first so that Dagobah system cleanup can
            # tell Yoda that ObiWan is gone.

            # Verify that the convention deregistration occurred

            for X in range(60):
                registrations2 = asys.ask(watcher, 'notifications', 1)
                print(str(registrations2))
                if 3 == len(registrations2):
                    break
                sleep(0.01)  # wait for Endor system to exit and deregister
            assert 3 == len(registrations2)
            for each in registrations:
                assert isinstance(each.remoteAdminAddress, ActorAddress)
                assert isinstance(each.remoteCapabilities, dict)
            assert 2 == sum([{True:1, False:0}[R.remoteAdded]
                             for R in registrations])

            # Verify that destroying the Endor system shutdown all Actors within it
            r = asys.ask(yoda, 'Training Completed?', 2)
            assert  (2,2) == r

        finally:
            if dagobah: dagobah.shutdown()
            if endor: endor.shutdown()
