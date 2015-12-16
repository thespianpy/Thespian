"""Verify Actor Status behavior.

The ThespianStatus request can be sent to any Actor in the system to
retrieve internal information about that Actor.

Note that these messages are internal to Thespian and not generally
available or useable, so they are not in the thespian/actors.py
definition file.

"""

import unittest
from datetime import datetime, timedelta
import time
import thespian.test.helpers
from thespian.actors import *
from thespian.system.messages.status import *
from thespian.test import ActorSystemTestCase

import datetime


class TestActor(Actor):

    def receiveMessage(self, msg, sender):
        if msg == 'NewChild':
            self.child = self.createActor(TestActor)
            self.send(sender, self.child)
        elif msg == 'Sleep':
            self.wakeupAfter(timedelta(seconds=10))
        print('TestActor got %s from %s'%(str(msg), str(sender)))


class TestASimpleSystem(ActorSystemTestCase):
    testbase='Simple'
    scope='func'

    def testGetStatsFromIdlePrimaryActor(self):
        aS = ActorSystem()
        aa = aS.createActor(TestActor)
        rsp = aS.ask(aa, Thespian_StatusReq(), 4)
        formatStatus(rsp)
        self.assertIsInstance(rsp, Thespian_ActorStatus)
        self.assertEqual(len(rsp.pendingMessages), 0)
        self.assertEqual(len(rsp.pendingWakeups), 0)
        self.assertEqual(len(rsp.childActors), 0)


    def testGetStatsShowsCorrectChildCount(self):
        aS = ActorSystem()
        aa = aS.createActor(TestActor)
        ab = aS.ask(aa, 'NewChild', 1)

        rsp = aS.ask(aa, Thespian_StatusReq(), 4)
        formatStatus(rsp)
        self.assertIsInstance(rsp, Thespian_ActorStatus)
        self.assertEqual(len(rsp.pendingMessages), 0)
        self.assertEqual(len(rsp.pendingWakeups), 0)
        self.assertEqual(len(rsp.childActors), 1)
        self.assertEqual(rsp.childActors[0], ab)

        rsp = aS.ask(ab, Thespian_StatusReq(), 4)
        formatStatus(rsp)
        self.assertIsInstance(rsp, Thespian_ActorStatus)
        self.assertEqual(len(rsp.pendingMessages), 0)
        self.assertEqual(len(rsp.pendingWakeups), 0)
        self.assertEqual(len(rsp.childActors), 0)

        ac = aS.ask(aa, 'NewChild', 1)

        rsp = aS.ask(aa, Thespian_StatusReq(), 4)
        formatStatus(rsp)
        self.assertIsInstance(rsp, Thespian_ActorStatus)
        self.assertEqual(len(rsp.pendingMessages), 0)
        self.assertEqual(len(rsp.pendingWakeups), 0)
        self.assertEqual(len(rsp.childActors), 2)
        self.assertIn(ab, rsp.childActors)
        self.assertIn(ac, rsp.childActors)

        aS.tell(ab, ActorExitRequest())  # parent loses a child
        ad = aS.ask(ac, 'NewChild', 1)   # parent doesn't see this

        rsp = aS.ask(aa, Thespian_StatusReq(), 4)
        formatStatus(rsp)
        self.assertIsInstance(rsp, Thespian_ActorStatus)
        self.assertEqual(len(rsp.pendingMessages), 0)
        self.assertEqual(len(rsp.pendingWakeups), 0)
        self.assertEqual(len(rsp.childActors), 1)
        self.assertIn(ac, rsp.childActors)


    def testGetStatsShowsCorrectSleepCount(self):
        aS = ActorSystem()
        aa = aS.createActor(TestActor)
        aS.tell(aa, 'Sleep')
        time.sleep(0.1)

        rsp = aS.ask(aa, Thespian_StatusReq(), 3)
        formatStatus(rsp)
        self.assertIsInstance(rsp, Thespian_ActorStatus)
        self.assertEqual(len(rsp.pendingMessages), 0)
        self.assertEqual(len(rsp.pendingWakeups), 1)
        self.assertEqual(len(rsp.childActors), 0)

        aS.tell(aa, 'Sleep')
        time.sleep(0.1)

        rsp = aS.ask(aa, Thespian_StatusReq(), 3)
        formatStatus(rsp)
        self.assertIsInstance(rsp, Thespian_ActorStatus)
        self.assertEqual(len(rsp.pendingMessages), 0)
        self.assertEqual(len(rsp.pendingWakeups), 2)
        self.assertEqual(len(rsp.childActors), 0)


class TestMultiprocUDPSystem(TestASimpleSystem):
    testbase='MultiprocUDP'
    def setUp(self):
        self.setSystemBase('multiprocUDPBase')
        super(TestMultiprocUDPSystem, self).setUp()

class TestMultiprocTCPSystem(TestASimpleSystem):
    testbase='MultiprocTCP'
    def setUp(self):
        self.setSystemBase('multiprocTCPBase')
        super(TestMultiprocTCPSystem, self).setUp()

class TestMultiprocQueueSystem(TestASimpleSystem):
    testbase='MultiprocQueue'
    def setUp(self):
        self.setSystemBase('multiprocQueueBase')
        super(TestMultiprocQueueSystem, self).setUp()

