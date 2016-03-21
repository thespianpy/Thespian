import unittest
import logging
import time, datetime
import thespian.test.helpers
from thespian.actors import *
from thespian.test import ActorSystemTestCase
import signal
import os


class KillMeActor(Actor):
    def receiveMessage(self, msg, sender):
        logging.info('EchoActor got %s (%s) from %s', msg, type(msg), sender)
        self.send(sender, os.getpid())


smallwait = datetime.timedelta(milliseconds=50)


class TestASimpleSystem(ActorSystemTestCase):
    testbase='Simple'
    scope='func'

    def testCreateActorSystem(self):
        pass

    def testSimpleActor(self):
        killme = ActorSystem().createActor(KillMeActor)

    def testSigCont(self):
        killme = ActorSystem().createActor(KillMeActor)
        killme_pid  = ActorSystem().ask(killme, 'pid?', smallwait)
        self.assertTrue(killme_pid)  # not 0 or None
        os.kill(killme_pid, signal.SIGCONT)
        self.assertEqual(killme_pid, ActorSystem().ask(killme, 'pid again?', smallwait))


class MultiProcTests(object):
    def _handle_non_deadly_signal(self, signum):
        killme = ActorSystem().createActor(KillMeActor)
        killme_pid  = ActorSystem().ask(killme, 'pid?', smallwait)
        self.assertTrue(killme_pid)  # not 0 or None
        os.kill(killme_pid, signum)
        time.sleep(0.02) # allow signal to be delivered
        self.assertEqual(killme_pid, ActorSystem().ask(killme, 'pid again?', smallwait))

    def _handle_deadly_signal(self, signum):
        killme = ActorSystem().createActor(KillMeActor)
        killme_pid  = ActorSystem().ask(killme, 'pid?', smallwait)
        self.assertTrue(killme_pid)  # not 0 or None
        os.kill(killme_pid, signum)
        time.sleep(0.02) # allow signal to be delivered
        self.assertIsNone(ActorSystem().ask(killme, 'pid again?', smallwait))

    def testSigInt(self):  self._handle_non_deadly_signal(signal.SIGINT)
    def testSigUsr1(self): self._handle_non_deadly_signal(signal.SIGUSR1)
    def testSigUsr2(self): self._handle_non_deadly_signal(signal.SIGUSR2)
    def testSigHup(self):  self._handle_non_deadly_signal(signal.SIGHUP)

    def testSigTerm(self): self._handle_deadly_signal(signal.SIGTERM)
    def testSigQuit(self): self._handle_deadly_signal(signal.SIGQUIT)
    def testSigAbrt(self): self._handle_deadly_signal(signal.SIGABRT)


class TestMultiprocUDPSystem(TestASimpleSystem, MultiProcTests):
    testbase='MultiprocUDP'
    def setUp(self):
        self.setSystemBase('multiprocUDPBase')
        super(TestMultiprocUDPSystem, self).setUp()

class TestMultiprocTCPSystem(TestASimpleSystem, MultiProcTests):
    testbase='MultiprocTCP'
    def setUp(self):
        self.setSystemBase('multiprocTCPBase')
        super(TestMultiprocTCPSystem, self).setUp()

class TestMultiprocQueueSystem(TestASimpleSystem, MultiProcTests):
    testbase='MultiprocQueue'
    def setUp(self):
        self.setSystemBase('multiprocQueueBase')
        super(TestMultiprocQueueSystem, self).setUp()

