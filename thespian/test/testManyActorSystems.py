import unittest
from time import sleep
import logging
from thespian.test import ActorSystemTestCase, simpleActorTestLogging
import thespian.test.helpers
from thespian.actors import *
from multiprocessing import Process, Pipe


def remoteSystem(conn, systembase, adminPort, systemCapabilities):
    # shutdown anything still running
    ActorSystem(logDefs = simpleActorTestLogging()).shutdown()

    conn.recv()  # wait for startup indication from parent

    # Setup capabilities with defaults overridded by passed-in specifications
    caps = {}
    caps.update({ 'Admin Port': adminPort,
                  'Convention Address.IPv4': ('', 12121),
              })
    caps.update(systemCapabilities)
    ActorSystem(systembase, caps,
                logDefs = simpleActorTestLogging())
    if 'DeathStar' != conn.recv():  # wait for shutdown indication from parent
        ActorSystem().shutdown()
    conn.close()
    return False


def DagobahSystem(conn, base): remoteSystem(conn, base, 16230, { 'Swamp': True })
def HothSystem(conn, base):    remoteSystem(conn, base, 16231, { 'Snow':  True })
def EndorSystem(conn, base):   remoteSystem(conn, base, 16232, { 'Trees': True })
def NabooSystem(conn, base):   remoteSystem(conn, base, 16233, { 'Ocean': True })


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


class TestManyActorSystem(unittest.TestCase):
    testbase='MultiprocTCP'
    scope='func'

    actorSystemBase = 'multiprocTCPBase'

    def setUp(self):
        self.systems = {}

        # Start sub-processes before primary ActorSystem so that the
        # latter is not duplicated in each sub-process.

        child_conn, parent_conn = Pipe()
        child = Process(target=DagobahSystem, args=(child_conn, self.actorSystemBase))
        child.start()
        self.systems['Dagobah'] = (parent_conn, child)

        child_conn, parent_conn = Pipe()
        child = Process(target=HothSystem, args=(child_conn, self.actorSystemBase))
        child.start()
        self.systems['Hoth'] = (parent_conn, child)

        child_conn, parent_conn = Pipe()
        child = Process(target=EndorSystem, args=(child_conn, self.actorSystemBase))
        child.start()
        self.systems['Endor'] = (parent_conn, child)

        child_conn, parent_conn = Pipe()
        child = Process(target=NabooSystem, args=(child_conn, self.actorSystemBase))
        child.start()
        self.systems['Naboo'] = (parent_conn, child)

        ActorSystem(self.actorSystemBase, {'Jedi Council': True,
                                           'Admin Port': 12121},
                    logDefs = simpleActorTestLogging())

        for each in self.systems:
            self.systems[each][0].send('Start now please')
        sleep(1)  # allow all Systems to startup


    def tearDown(self):
        for system in self.systems:
            self.systems[system][0].send('OK, all done')
        for system in self.systems:
            for x in range(10):
                if not self.systems[system][1].is_alive():
                    break
                sleep(1)
        ActorSystem().shutdown()


    def test00_primeLogging(self):
        pass


    def test01_checkAllSystemStart(self):
        pass


    def test02_checkAllSystemAndActorsStart(self):
        ewok     = ActorSystem().createActor(Ewok)
        jarjar   = ActorSystem().createActor(JarJar)
        tauntaun = ActorSystem().createActor(Tauntaun)
        yoda     = ActorSystem().createActor(Yoda)
        sleep(1)

    def test03_checkAllSystemsAndActorsStart(self):
        ewok     = ActorSystem().createActor(Ewok)
        jarjar   = ActorSystem().createActor(JarJar)
        tauntaun = ActorSystem().createActor(Tauntaun)
        yoda     = ActorSystem().createActor(Yoda)
        self.assertEqual(ActorSystem().ask(ewok, 'hi', 0.25), 'We cook hi for dinner')
        self.assertEqual(ActorSystem().ask(yoda, 'hi', 0.25), 'Use the Force, you must, to hi')
        self.assertEqual(ActorSystem().ask(jarjar, 'hi', 0.25), 'hi?  How rude!')
        self.assertEqual(ActorSystem().ask(tauntaun, 'hi', 0.25), 'waagh! hi')

    def test04_conversation(self):
        ewok     = ActorSystem().createActor(Ewok)
        jarjar   = ActorSystem().createActor(JarJar)
        tauntaun = ActorSystem().createActor(Tauntaun)
        yoda     = ActorSystem().createActor(Yoda)
        minutes = ActorSystem().ask(ewok,
                                    Conversation('hi',
                                                 [yoda, jarjar, tauntaun]), 0.25)
        self.assertIsNotNone(minutes)
        self.assertEqual(minutes.results,
                         [
                             'We cook hi for dinner',
                             'waagh! hi',
                             'hi?  How rude!',
                             'Use the Force, you must, to hi',
                         ])

        minutes2 = ActorSystem().ask(yoda,
                                    Conversation('hi',
                                                 [yoda, jarjar, jarjar, ewok, tauntaun, ewok, tauntaun]), 0.25)
        self.assertIsNotNone(minutes2)
        self.assertEqual(minutes2.results,
                         [
                             'Use the Force, you must, to hi',
                             'waagh! hi',
                             'We cook hi for dinner',
                             'waagh! hi',
                             'We cook hi for dinner',
                             'hi?  How rude!',
                             'hi?  How rude!',
                             'Use the Force, you must, to hi',
                         ])

    def test05_uniqueAddresses(self):
        ewok     = ActorSystem().createActor(Ewok)
        jarjar   = ActorSystem().createActor(JarJar)
        tauntaun = ActorSystem().createActor(Tauntaun)
        yoda     = ActorSystem().createActor(Yoda)

        self.assertIsNotNone(ActorSystem().ask(ewok, WhoAreYou(), 0.25))
        self.assertIsNotNone(ActorSystem().ask(jarjar, WhoAreYou(), 0.25))
        self.assertIsNotNone(ActorSystem().ask(tauntaun, WhoAreYou(), 0.25))
        self.assertIsNotNone(ActorSystem().ask(yoda, WhoAreYou(), 0.25))

        self.assertNotEqual(ActorSystem().ask(ewok, WhoAreYou(), 0.25),
                            ActorSystem().ask(jarjar, WhoAreYou(), 0.25))
        self.assertNotEqual(ActorSystem().ask(ewok, WhoAreYou(), 0.25),
                            ActorSystem().ask(tauntaun, WhoAreYou(), 0.25))
        self.assertNotEqual(ActorSystem().ask(ewok, WhoAreYou(), 0.25),
                            ActorSystem().ask(yoda, WhoAreYou(), 0.25))
        self.assertNotEqual(ActorSystem().ask(jarjar, WhoAreYou(), 0.25),
                            ActorSystem().ask(tauntaun, WhoAreYou(), 0.25))
        self.assertNotEqual(ActorSystem().ask(jarjar, WhoAreYou(), 0.25),
                            ActorSystem().ask(yoda, WhoAreYou(), 0.25))
        self.assertNotEqual(ActorSystem().ask(tauntaun, WhoAreYou(), 0.25),
                            ActorSystem().ask(yoda, WhoAreYou(), 0.25))


    def test06_primaryActorRequirements(self):
        jarjar = ActorSystem().createActor(JarJar)  # on Naboo
        self.assertEqual(ActorSystem().ask(jarjar, 'hi', 0.25), 'hi?  How rude!')

        self.systems['Naboo'][0].send('OK, all done')
        ref = self.systems['Naboo'][1]
        del self.systems['Naboo']
        for x in range(20):
            if not ref.is_alive():
                break
            sleep(1)
        sleep(0.15)

        try:
            self.assertEqual(ActorSystem().ask(jarjar, 'hi again', 0.25), None) # JarJar is gone
        except ActorSystemFailure:
            # Not all ActorSystems fail this way, but most will
            pass

        # Create a new JarJar on Endor
        jarjar = ActorSystem().createActor(
            JarJar,
            targetActorRequirements={'WantsToSee': 'Trees',
                                     'TravellingWith': 'ObiWan'})
        self.assertEqual(ActorSystem().ask(jarjar, 'hiya', 0.25), 'hiya?  How rude!')

        self.systems['Endor'][0].send('OK, all done')
        for x in range(10):
            if not self.systems['Endor'][1].is_alive():
                break
            sleep(1)
        del self.systems['Endor']
        sleep(0.15)

        try:
            self.assertEqual(ActorSystem().ask(jarjar, 'hi ho', 0.25), None) # JarJar is gone
        except ActorSystemFailure:
            # Not all ActorSystems fail this way, but most will
            pass

        # Now specify requirements no system can meet: JarJar is not created
        try:
            jarjar = ActorSystem().createActor(JarJar,
                                               targetActorRequirements={'WantsToSee': 'Trees'})
            self.assertEqual(jarjar, None)
        except NoCompatibleSystemForActor:
            # This is a valid, alternate behavior for some ActorSystems
            pass

        # Now bring JarJar to Hoth
        jarjar = ActorSystem().createActor(JarJar,
                                           targetActorRequirements={'WantsToSee': 'Snow'})
        self.assertEqual(ActorSystem().ask(jarjar, 'hi you', 0.25), 'hi you?  How rude!')


    def test07_subActorRequirements(self):
        # JarJar's home world is taken by the Empire
        self.systems['Naboo'][0].send('OK, all done')
        for x in range(10):
            if not self.systems['Naboo'][1].is_alive():
                break
            sleep(1)
        del self.systems['Naboo']
        sleep(0.15)

        # JarJar can't go home anymore
        try:
            jarjar = ActorSystem().createActor(JarJar)
            self.assertEqual(jarjar, None)
        except NoCompatibleSystemForActor:
            # This is a valid, alternate behavior for some ActorSystems
            pass

        ewok = ActorSystem().createActor(Ewok)
        self.assertEqual(ActorSystem().ask(ewok, 'beans', 0.25), 'We cook beans for dinner')

        # Ewok creates a JarJar actor in the same ActorSystem (Endor)
        self.assertEqual(ActorSystem().ask(ewok, 'Greet JarJar', 0.35), 'Trees?  Huh?')

        # Ewok creats a JarJar actor but requires it to be in the Hoth ActorSystem
        self.assertEqual(ActorSystem().ask(ewok, 'Freeze JarJar', 1.35), 'Snow?  Huh?')


    def test08_requiredRequirementsNotPassedFails(self):
        try:
            g1 = ActorSystem().createActor(Guest)
            self.assertEqual(g1, None)
        except NoCompatibleSystemForActor:
            # Confirmed: Guest must be invited
            pass

    def test09_requiredRequirementsPassedToTopLevelSucceeds(self):
        g1 = ActorSystem().createActor(Guest, targetActorRequirements={'Invited to': 'Ocean'})
        self.assertEqual(ActorSystem().ask(g1, 'the seashell', 0.25),
                         'Thank you for the seashell')

    def test10_requiredRequirementsPassedViaSubLevelSucceeds(self):
        ewok = ActorSystem().createActor(Ewok)
        self.assertEqual(ActorSystem().ask(ewok, 'beans', 0.25), 'We cook beans for dinner')

        # Ewok creates a Guest actor in the same ActorSystem (Endor)
        self.assertEqual(ActorSystem().ask(ewok, 'Greet Guest', 0.25), 'Thanks for the Trees')


class TestMultiprocUDPSystem(TestManyActorSystem):
    testbase='MultiprocUDP'
    scope='func'

    actorSystemBase = 'multiprocUDPBase'


# ----------------------------------------------------------------------

class Notified(Actor):
    def receiveMessage(self, msg, sender):
        logging.info('Notified got: %s', str(msg))
        if msg == 'register':
            self.notifications = []
            self.notifyOnSystemRegistrationChanges(True)
        elif isinstance(msg, ActorSystemConventionUpdate):
            self.notifications.append('%s %s'%(('IN' if msg._added else 'OUT'),
                                               str(msg._remoteAdminAddress)))
        elif msg == 'notifications':
            self.send(sender, '&'.join(self.notifications))


class TestConventionWatcher(unittest.TestCase):
    testbase='MultiprocTCP'
    scope='func'

    actorSystemBase = 'multiprocTCPBase'

    def testNotifications(self):

        systems = {}

        try:

            # Start sub-processes before ActorSystem so that the
            # ActorSystem doesn't get duplicated in all the
            # sub-processes.  The sub-processes will wait for a
            # startup message from this process before creating their
            # ActorSystems.

            parent_conn1, child_conn1 = Pipe()
            child1 = Process(target=DagobahSystem, args=(child_conn1, self.actorSystemBase))
            child1.start()
            systems['Dagobah'] = (parent_conn1, child1)

            child_conn2, parent_conn2 = Pipe()
            child2 = Process(target=HothSystem, args=(child_conn2, self.actorSystemBase))
            child2.start()
            systems['Hoth'] = (parent_conn2, child2)

            child_conn3, parent_conn3 = Pipe()
            child3 = Process(target=EndorSystem, args=(child_conn3, self.actorSystemBase))
            child3.start()
            systems['Endor'] = (parent_conn3, child3)

            child_conn4, parent_conn4 = Pipe()
            child4 = Process(target=NabooSystem, args=(child_conn4, self.actorSystemBase))
            child4.start()
            systems['Naboo'] = (parent_conn4, child4)

            # Start the Primary ActorSystem and an Actor that
            # registers for Convention entry/exit from other
            # ActorSystems.

            ActorSystem(self.actorSystemBase, {'Jedi Council': True,
                                               'Admin Port': 12121},
                        logDefs =  simpleActorTestLogging())

            watcher = ActorSystem().createActor(Notified)
            ActorSystem().tell(watcher, 'register')
            sleep(0.10)  # wait for watcher to register

            # Now start each of the secondary ActorSystems; their
            # registration should be noted by the Actor registered for
            # such notifications.

            for each in systems:
                systems[each][0].send('Start now please')

            # Verify all anticipated registrations actually occurred.

            for X in range(30):
                registrations = ActorSystem().ask(watcher, 'notifications', 1).split('&')
                print(registrations)
                if 4 == len(registrations):
                    break
                sleep(0.01)  # wait for more registrations to complete
            self.assertEqual(4, len(registrations))

            # Now ask an ActorSystem to exit

            systems['Hoth'][0].send('OK, all done')
            del systems['Hoth']

            # Verify that the convention deregistration occurred

            for X in range(30):
                registrations2 = ActorSystem().ask(watcher, 'notifications', 1).split('&')
                if 5 == len(registrations2):
                    break
                sleep(0.01)  # wait for Hoth system to exit and deregister
            self.assertEqual(5, len(registrations2))

            outs = [X for X in registrations2 if X.startswith('OUT')]
            self.assertEqual(1, len(outs))

        finally:
            for system in systems:
                systems[system][0].send('OK, all done')
            sleep(0.1)  # allow other actorsystems (non-convention-leaders) to exit
            ActorSystem().shutdown()


class TestMultiprocUDPSystemWatcher(TestConventionWatcher):
    testbase='MultiprocUDP'
    scope='func'

    actorSystemBase = 'multiprocUDPBase'


class TestConventionDeregistration(unittest.TestCase):
    testbase='MultiprocTCP'
    scope='func'

    actorSystemBase = 'multiprocTCPBase'

    def testNotifications(self):

        systems = {}

        try:

            # Start sub-processes before ActorSystem so that the
            # ActorSystem doesn't get duplicated in all the
            # sub-processes.  The sub-processes will wait for a
            # startup message from this process before creating their
            # ActorSystems.

            parent_conn1, child_conn1 = Pipe()
            child1 = Process(target=DagobahSystem, args=(child_conn1, self.actorSystemBase))
            child1.start()
            systems['Dagobah'] = (parent_conn1, child1)

            parent_conn1, child_conn1 = Pipe()
            child1 = Process(target=EndorSystem, args=(child_conn1, self.actorSystemBase))
            child1.start()
            systems['Endor'] = (parent_conn1, child1)

            # Start the Primary ActorSystem and an Actor that
            # registers for Convention entry/exit from other
            # ActorSystems.

            ActorSystem(self.actorSystemBase, {'Jedi Council': True,
                                               'Admin Port': 12121},
                        logDefs =  simpleActorTestLogging())

            watcher = ActorSystem().createActor(Notified)
            ActorSystem().tell(watcher, 'register')
            sleep(0.2)  # wait for watcher to register

            # Now start each of the secondary ActorSystems; their
            # registration should be noted by the Actor registered for
            # such notifications.

            for each in systems:
                systems[each][0].send('Start now please')

            # Verify all anticipated registrations actually occurred.

            for X in range(50):
                registrations = ActorSystem().ask(watcher, 'notifications', 1).split('&')
                print(registrations)
                if 2 == len(registrations):
                    break
                sleep(0.01)    # wait for systems to startup and register
            self.assertEqual(2, len(registrations))

            # Now there are 3 actor Systems:
            #    Jedi Council (convention leader)
            #    Endor (Trees)
            #    Dagobah (Swamp)
            # Create some Actors:
            #    Yoda (from Primary, created in system Dagobah)
            #       ObiWan  (from Yoda, through Jedi Council to system Endor)
            #       Luke    (from Yoda, but cannot start this anywhere)
            # Verify that ObiWan starts and stays started, but that Luke "starts" and subsequently exits.

            yoda = ActorSystem().createActor(Yoda)
            self.assertEqual('Use the Force, you must, to train', ActorSystem().ask(yoda, 'train', 2))
            self.assertEqual( (0,0), ActorSystem().ask(yoda, 'Training Completed?', 2))
            ActorSystem().tell(yoda, 'Obi Wan')
            ActorSystem().tell(yoda, 'Padawan')
            sleep(0.25)  # allow time for Yoda to fail training a young Padawan
            self.assertEqual( (2,1), ActorSystem().ask(yoda, 'Training Completed?', 2))

            # Now ask an ActorSystem to exit.  This is the ActorSystem
            # where Obi Wan is, so that will cause Obi Wan to go away
            # as well.

            systems['Endor'][0].send('Please exit nicely')
            del systems['Endor']

            # KWQ: how to get Endor to abruptly exit without shutting
            # down ObiWan first so that Dagobah system cleanup can
            # tell Yoda that ObiWan is gone.

            # Verify that the convention deregistration occurred

            for X in range(60):
                registrations2 = ActorSystem().ask(watcher, 'notifications', 1).split('&')
                print(str(registrations2))
                if 3 == len(registrations2):
                    break
                sleep(0.01)  # wait for Endor system to exit and deregister
            self.assertEqual(3, len(registrations2))

            outs = [X for X in registrations2 if X.startswith('OUT')]
            self.assertEqual(1, len(outs))

            # Verify that destroying the Endor system shutdown all Actors within it
            self.assertEqual( (2,2), ActorSystem().ask(yoda, 'Training Completed?', 2))

        finally:
            for system in systems:
                systems[system][0].send('OK, all done')
            sleep(0.3)  # allow other actorsystems (non-convention-leaders) to exit
            ActorSystem().shutdown()


class TestMultiprocUDPSystemDeregistration(TestConventionDeregistration):
    testbase='MultiprocUDP'
    scope='func'

    actorSystemBase = 'multiprocUDPBase'
