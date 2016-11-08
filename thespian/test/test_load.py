"""Load testing for Actor System

This generates a high load through a set of Actors to verify that the
Actors can handle the load.

Additionally, this can be run directly from the command line to get load results.

"""

import logging
import logging.handlers
from datetime import datetime, timedelta
from thespian.actors import *
import random
from thespian.test import *
from thespian.system.timing import timePeriodSeconds
import pytest


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


# n.b.  multiprocQueueBase tends to deadlock, especially in testSamePathLengthTenAsking


@pytest.fixture(params=[5,20,50])
def nMessages(request):
    return request.param


@pytest.fixture(params=[2,3,5])
def actorDepth(request):
    return request.param


class TestFuncLoad(object):

    def testSamePathLengthTenAndDiscard(self, asys, nMessages, actorDepth):
        unstable_test(asys, 'multiprocQueueBase')
        nodoc = 1
        """This test pushes a large number of messages into the system and
           does not wait for them to be processed.  If nMessages is
           set too high (> 2500), the ActorExitRequests will not be
           sent and Actors may be leftover.
        """
        actors = [asys.createActor(TestActor) for X in range(actorDepth)]
        starttime = datetime.now()
        for X in range(nMessages):  # messages to send
            msg = TestMessage(actors[1:])
            msg.origSender = None
            asys.tell(actors[0], msg)
        endtime = datetime.now()
        elapsed = endtime - starttime
        report('Discarding', getattr(asys, 'name', asys.base_name), elapsed, nMessages, actorDepth)
        [asys.tell(A, ActorExitRequest()) for A in actors]


    def testSamePathLengthTenAndDiscardAndFinalAsk(self, asys, nMessages, actorDepth):
        unstable_test(asys, 'multiprocQueueBase')
        nodoc = 1
        """This test pushes a large number of messages into the system and
           does not wait for them to be processed.  At the end, it
           asks for a response on a final message with a 30 second
           timeout.  Unreliable transports (e.g. UDP) will probably
           drop some messages under a high message count load and end
           up timing out.
        """
        actors = [asys.createActor(TestActor) for X in range(actorDepth)]
        for ii,actor in enumerate(actors):
            if actor is None or not actor:
                raise ValueError('Actor address %s is %s!'%(ii, actor))
        starttime = datetime.now()
        for X in range(nMessages):  # messages to send
            msg = TestMessage(actors[1:])
            msg.origSender = None
            asys.tell(actors[0], msg)

        msg = TestMessage(actors[1:])
        rmsg = asys.ask(actors[0], msg, 30)
        endtime = datetime.now()
        elapsed = endtime - starttime
        report('Discard, Last Ask (%s)'%(rmsg.sum if rmsg else '<timeout>'),
               getattr(asys, 'name', asys.base_name), elapsed,
               nMessages, actorDepth)
        [asys.tell(A, ActorExitRequest()) for A in actors]


    def testSamePathLengthTenAsking(self, asys, nMessages, actorDepth):
        unstable_test(asys, 'multiprocQueueBase')
        nodoc = 1
        """This test pushes a large number of messages into the system and
           does not wait for them to be processed.  If nMessages is
           set too high (> 2500), the ActorExitRequests will not be
           sent and Actors may be leftover.
        """
        actors = [asys.createActor(TestActor) for X in range(actorDepth)]
        starttime = datetime.now()
        for X in range(nMessages):  # messages to send
            msg = TestMessage(actors[1:])
            asys.ask(actors[0], msg, 30)
        endtime = datetime.now()
        elapsed = endtime - starttime
        report('Asking', getattr(asys, 'name', asys.base_name), elapsed, nMessages, actorDepth)
        [asys.tell(A, ActorExitRequest()) for A in actors]



noLogging = { 'version' : 1,
              # n.b. NullHandler is not available until Python 2.7
              'handlers': { 'discarder': { 'class': 'logging.StreamHandler',
                                           'stream': open('/dev/null','w'),
                                       } },
              'root': { 'level': 'CRITICAL', 'handlers' : ['discarder'] },
              'disable_existing_loggers': True,
              }

testAdminPort = 59300

def start_actor_system(name):
    caps = {}
    if name.startswith('multiprocTCP') or \
       name.startswith('multiprocUDP'):
        global testAdminPort
        caps['Admin Port'] = testAdminPort
        testAdminPort = testAdminPort + 1
    if name.endswith('-AdminRouting'):
        caps['Admin Routing'] = True
    asys = ActorSystem(systemBase=name.partition('-')[0],
                       capabilities=caps,
                       logDefs=(simpleActorTestLogging()
                                if name.startswith('multiproc')
                                else False),
                       transientUnique=True)
    asys.base_name = name
    return asys


def stop_actor_system(asys):
    asys.shutdown()


if __name__ == "__main__":

    for tm in [N for N in dir(TestFuncLoad) if N.startswith('test')]:
        for tbase in [
                'simpleSystemBase',
                'multiprocUDPBase',
                'multiprocTCPBase',
                'multiprocTCPBase-AdminRouting',
                # multiprocQueueBase seems to get into a semaphore
                # deadlock in multiprocessing/queues.py (under
                # Python 2.6)
                # 'multiprocQueueBase',
        ]:
            tci = TestFuncLoad()
            asys = start_actor_system(tbase)
            try:
                getattr(tci,tm)(asys, 2000, 10)
            finally:
                startstop = datetime.now()
                stop_actor_system(asys)
                endstop = datetime.now()
            stoptime = endstop - startstop
            if stoptime > timedelta(seconds=2):
                print('  [%s stoptime: %s]'%(tbase, str(stoptime)))

