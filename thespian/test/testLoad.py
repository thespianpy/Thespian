"""Load testing for Actor System

This generates a high load through a set of Actors to verify that the
Actors can handle the load.

Additionally, this can be run directly from the command line to get load results.

"""

import unittest
import logging
import logging.handlers
from datetime import datetime, timedelta
import thespian.test.helpers
from thespian.actors import *
import random
from thespian.test import ActorSystemTestCase, LocallyManagedActorSystem
from thespian.system.utilis import timePeriodSeconds


class TestMessage(object):
    count = 0
    def __init__(self, routing):
        self.routing = routing
        self.name = 'TestMessage.%s'%self.count
        TestMessage.count += 1
        self.sum  = 0
    def __str__(self): return self.name


class TestActor(Actor):

    def __init__(self):
        self.sum = 0

    def receiveMessage(self, msg, sender):
        if isinstance(msg, TestMessage):
            if not hasattr(msg, 'origSender'):
                msg.origSender = sender
            self.sum = self.sum + 1
            next = msg.routing.pop(0) if msg.routing else msg.origSender
            if next:
                msg.sum = self.sum
                self.send(next, msg)


def report(what, sysBase, elapsed, nMessages, nActors):
    print('%5d Actors, %30s %22s -- %s -- %5.2f/sec overall -- %7.2f msg/sec'%(
        nActors, what, sysBase, elapsed,
        nMessages / timePeriodSeconds(elapsed),
        nMessages * nActors / timePeriodSeconds(elapsed)))


class LoadTester(LocallyManagedActorSystem):

    def prepSys(self, systemBase, logDefs=None, extraCapabilities=None, name=None):
        self.sysBase = systemBase
        capabilities = {'Admin Port': int(random.uniform(3000, 64000))}
        if extraCapabilities: capabilities.update(extraCapabilities)
        self.setSystemBase(systemBase,
                           systemCapabilities = capabilities,
                           logDefs = logDefs)
        self.name = name or systemBase

    def endSys(self):  ActorSystem().shutdown()

    def testSamePathLengthTenAndDiscard(self):
        nodoc = 1
        """This test pushes a large number of messages into the system and
           does not wait for them to be processed.  If nMessages is
           set too high (> 2500), the ActorExitRequests will not be
           sent and Actors may be leftover.
        """
        nMessages = 2000
        actorDepth = 10

        aS = ActorSystem()
        actors = [aS.createActor(TestActor) for X in range(actorDepth)]
        starttime = datetime.now()
        for X in range(nMessages):  # messages to send
            msg = TestMessage(actors[1:])
            msg.origSender = None
            aS.tell(actors[0], msg)
        endtime = datetime.now()
        elapsed = endtime - starttime
        report('Discarding', self.name, elapsed, nMessages, actorDepth)
        [aS.tell(A, ActorExitRequest()) for A in actors]


    def testSamePathLengthTenAndDiscardAndFinalAsk(self):
        nodoc = 1
        """This test pushes a large number of messages into the system and
           does not wait for them to be processed.  At the end, it
           asks for a response on a final message with a 30 second
           timeout.  Unreliable transports (e.g. UDP) will probably
           drop some messages under a high message count load and end
           up timing out.
        """
        nMessages = 2000
        actorDepth = 10

        aS = ActorSystem()
        actors = [aS.createActor(TestActor) for X in range(actorDepth)]
        for ii,actor in enumerate(actors):
            if actor is None or not actor:
                raise ValueError('Actor address %s is %s!'%(ii, actor))
        starttime = datetime.now()
        for X in range(nMessages):  # messages to send
            msg = TestMessage(actors[1:])
            msg.origSender = None
            aS.tell(actors[0], msg)

        msg = TestMessage(actors[1:])
        rmsg = aS.ask(actors[0], msg, 30)
        endtime = datetime.now()
        elapsed = endtime - starttime
        report('Discard, Last Ask (%s)'%(rmsg.sum if rmsg else '<timeout>'),
               self.name, elapsed,
               nMessages, actorDepth)
        [aS.tell(A, ActorExitRequest()) for A in actors]


    def testSamePathLengthTenAsking(self):
        nodoc = 1
        """This test pushes a large number of messages into the system and
           does not wait for them to be processed.  If nMessages is
           set too high (> 2500), the ActorExitRequests will not be
           sent and Actors may be leftover.
        """
        nMessages = 2000
        actorDepth = 10

        aS = ActorSystem()
        actors = [aS.createActor(TestActor) for X in range(actorDepth)]
        starttime = datetime.now()
        for X in range(nMessages):  # messages to send
            msg = TestMessage(actors[1:])
            aS.ask(actors[0], msg, 30)
        endtime = datetime.now()
        elapsed = endtime - starttime
        report('Asking', self.name, elapsed, nMessages, actorDepth)
        [aS.tell(A, ActorExitRequest()) for A in actors]


class TestASimpleSystem(ActorSystemTestCase, LoadTester):
    testbase='Simple'
    scope='func'
    def setUp(self):    self.prepSys('simpleSystemBase')
    def tearDown(self): self.endSys()

class TestMultiprocUDPSystem(TestASimpleSystem):
    testbase='MultiprocUDP'
    def setUp(self):
        self.prepSys('multiprocUDPBase')

class TestMultiprocTCPSystem(TestASimpleSystem):
    testbase='MultiprocTCP'
    def setUp(self):
        self.prepSys('multiprocTCPBase')

class TestMultiprocTCPSystemAdminForwarding(TestASimpleSystem):
    testbase='MultiprocTCP'
    def setUp(self):
        self.prepSys('multiprocTCPBase',
                     extraCapabilities={'Admin Routing': True})

class TestMultiprocQueueSystem(TestASimpleSystem):
    testbase='MultiprocQueue'
    unstable=1  # tends to deadlock, especially in testSamePathLengthTenAsking
    def setUp(self):
        self.prepSys('multiprocQueueBase')
    def testSamePathLengthTenAndDiscard(self): pass
    def testSamePathLengthTenAndDiscardAndFinalAsk(self): pass

class TestMultiprocQueueSystemUnstable(TestASimpleSystem):
    testbase='MultiprocQueue'
    unstable=1
    def setUp(self):
        self.prepSys('multiprocQueueBase')
    def testSamePathLengthTenAsking(self): pass



noLogging = { 'version' : 1,
              # n.b. NullHandler is not available unti Python 2.7
              'handlers': { 'discarder': { 'class': 'logging.StreamHandler',
                                           'stream': open('/dev/null','w'),
                                       } },
              'root': { 'level': 'CRITICAL', 'handlers' : ['discarder'] },
              'disable_existing_loggers': True,
              }

if __name__ == "__main__":

    for tm in [N for N in dir(LoadTester) if N.startswith('test')]:
        for tbase,tcap,tname in [
                ('simpleSystemBase',{}, None),
                ('multiprocUDPBase',{}, None),
                ('multiprocTCPBase',{}, None),
                ('multiprocTCPBase', {'Admin Forwarding': True}, 'mpTCP-AdminFwd'),
                # multiprocQueueBase seems to get into a semaphore
                # deadlock in multiprocessing/queues.py (under
                # Python 2.6)
                # 'multiprocQueueBase',
        ]:
            tci = LoadTester()
            tci.prepSys(tbase, noLogging, extraCapabilities=tcap, name=tname) # each use diff admin

            getattr(tci,tm)()

            startstop = datetime.now()
            tci.endSys()
            endstop = datetime.now()
            stoptime = endstop - startstop
            if stoptime > timedelta(seconds=2):
                print('  [%s stoptime: %s]'%(tname or tbase, str(stoptime)))

