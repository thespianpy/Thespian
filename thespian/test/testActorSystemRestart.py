"""This test is for ensuring that any a new ActorSystem request will
   connect to a currently-running Actor System.
"""

import unittest
import thespian.test.helpers
from thespian.actors import *
from thespian.test import ActorSystemTestCase
import time
import multiprocessing

class FwdMsg(object):
    def __init__(self, path):
        self.path = path
        self.pathdone = []
    def next(self, sender):
        if not self.pathdone:
            self.path.insert(0, sender)
        self.pathdone.append(self.path.pop())
        return self.path[-1]


class Parent(Actor):
    def receiveMessage(self, msg, sender):
        if msg == 'Hello':
            self.send(sender, 'Hi')
        elif msg == 'Sleep':
            time.sleep(2)
        elif isinstance(msg, FwdMsg):
            tgt = msg.next(sender)
            self.send(tgt, msg)


class TestASimpleSystem(ActorSystemTestCase):
    testbase='Simple'
    scope='func'

    def testFwdMsg(self):
        aS = ActorSystem()
        a1 = aS.createActor(Parent)
        a2 = aS.createActor(Parent)
        r = aS.ask(a1, FwdMsg([a2,a1,a2,a2]), 0.5)
        self.assertEqual([a2,a2,a1,a2], r.pathdone)

    def testConnectToExistingActorSystem(self):
        as1 = ActorSystem()
        parent1 = as1.createActor(Parent)
        self.assertEqual('Hi', as1.ask(parent1, 'Hello', 3))

        # Access Internals to make singleton "forget" about the
        # current ActorSystem.
        ActorSystem.systemBase = None

        aS = ActorSystem(self.currentBase)
        try:

            parent = aS.createActor(Parent)
            self.assertEqual('Hi', aS.ask(parent, 'Hello', 3))

            r = aS.ask(parent, FwdMsg([parent1,parent,parent1]), 0.5)
            self.assertEqual([parent1,parent,parent1], r.pathdone)

        finally:
            pass
            aS.shutdown()

    def testConnectToStoppingActorSystem(self):
        as1 = ActorSystem()
        parent1 = as1.createActor(Parent)
        self.assertEqual('Hi', as1.ask(parent1, 'Hello', 3))
        as1.tell(parent1, 'Sleep')  # Parent will prevent shutdown for a little while
        p = multiprocessing.Process(target=stopAdmin, args=(as1,))

        p.start()

        # Access Internals to make singleton "forget" about the
        # current ActorSystem.
        ActorSystem.systemBase = None

        aS = ActorSystem(self.currentBase)
        try:
            parent = aS.createActor(Parent)
            # Should never get here...
            self.assertEqual('Hi', aS.ask(parent, 'Hello', 1))

        except ActorSystemFailure:
            pass
        except NoCompatibleSystemForActor:
            pass  # this is expected, although it takes a while to get (10s)
        finally:
            pass
            aS.shutdown()
            p.join()


def stopAdmin(actorsys):
    actorsys.shutdown()

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
