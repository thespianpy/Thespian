"""Verify Actor Status behavior.

The ThespianStatus request can be sent to any Actor in the system to
retrieve internal information about that Actor.

Note that these messages are internal to Thespian and not generally
available or useable, so they are not in the thespian/actors.py
definition file.

"""

from thespian.test import *
from datetime import datetime, timedelta
import time
from thespian.actors import *
from thespian.system.messages.status import *
import datetime


class TestActor(Actor):

    def receiveMessage(self, msg, sender):
        if msg == 'NewChild':
            self.child = self.createActor(TestActor)
            self.send(sender, self.child)
        elif msg == 'Sleep':
            self.wakeupAfter(timedelta(seconds=10))
        print('TestActor got %s from %s'%(str(msg), str(sender)))


class TestFuncStats(object):

    def testGetStatsFromIdlePrimaryActor(self, asys):
        aa = asys.createActor(TestActor)
        rsp = asys.ask(aa, Thespian_StatusReq(), 4)
        formatStatus(rsp)
        assert isinstance(rsp, Thespian_ActorStatus)
        assert len(rsp.pendingMessages) == 0
        assert len(rsp.pendingWakeups) == 0
        assert len(rsp.childActors) == 0


    def testGetStatsShowsCorrectChildCount(self, asys):
        aa = asys.createActor(TestActor)
        ab = asys.ask(aa, 'NewChild', 1)

        rsp = asys.ask(aa, Thespian_StatusReq(), 4)
        formatStatus(rsp)
        assert isinstance(rsp, Thespian_ActorStatus)
        assert len(rsp.pendingMessages) == 0
        assert len(rsp.pendingWakeups) == 0
        assert len(rsp.childActors) == 1
        assert rsp.childActors[0] == ab

        rsp = asys.ask(ab, Thespian_StatusReq(), 4)
        formatStatus(rsp)
        assert isinstance(rsp, Thespian_ActorStatus)
        assert len(rsp.pendingMessages) == 0
        assert len(rsp.pendingWakeups) == 0
        assert len(rsp.childActors) == 0

        ac = asys.ask(aa, 'NewChild', 1)

        rsp = asys.ask(aa, Thespian_StatusReq(), 4)
        formatStatus(rsp)
        assert isinstance(rsp, Thespian_ActorStatus)
        assert len(rsp.pendingMessages) == 0
        assert len(rsp.pendingWakeups) == 0
        assert len(rsp.childActors) == 2
        assert ab in rsp.childActors
        assert ac in rsp.childActors

        asys.tell(ab, ActorExitRequest())  # parent loses a child
        ad = asys.ask(ac, 'NewChild', 1)   # parent doesn't see this

        rsp = asys.ask(aa, Thespian_StatusReq(), 4)
        formatStatus(rsp)
        assert isinstance(rsp, Thespian_ActorStatus)
        assert len(rsp.pendingMessages) == 0
        assert len(rsp.pendingWakeups) == 0
        assert len(rsp.childActors) == 1
        assert ac in rsp.childActors


    def testGetStatsShowsCorrectSleepCount(self, asys):
        aa = asys.createActor(TestActor)
        asys.tell(aa, 'Sleep')
        time.sleep(0.1)

        rsp = asys.ask(aa, Thespian_StatusReq(), 3)
        formatStatus(rsp)
        assert isinstance(rsp, Thespian_ActorStatus)
        assert len(rsp.pendingMessages) == 0
        assert len(rsp.pendingWakeups) == 1
        assert len(rsp.childActors) == 0

        asys.tell(aa, 'Sleep')
        time.sleep(0.1)

        rsp = asys.ask(aa, Thespian_StatusReq(), 3)
        formatStatus(rsp)
        assert isinstance(rsp, Thespian_ActorStatus)
        assert len(rsp.pendingMessages) == 0
        assert len(rsp.pendingWakeups) == 2
        assert len(rsp.childActors) == 0
