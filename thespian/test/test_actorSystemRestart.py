"""This test is for ensuring that any a new ActorSystem request will
   connect to a currently-running Actor System.
"""

from thespian.actors import *
from thespian.test import *
import time
from datetime import timedelta


ask_wait = timedelta(seconds=8)


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

class SysStopper(Actor):
    def receiveMessage(self, msg, sender):
        if not isinstance(msg, ActorExitRequest):
            self.actorSystemShutdown()


class TestFuncSystemRestart(object):

    def testFwdMsg(self, asys):
        a1 = asys.createActor(Parent)
        a2 = asys.createActor(Parent)
        r = asys.ask(a1, FwdMsg([a2,a1,a2,a2]), ask_wait)
        assert [a2,a2,a1,a2] == r.pathdone

    def testConnectToExistingActorSystem(self, asys):
        actor_system_unsupported(asys, 'multiprocTCPBase-AdminRoutingTXOnly')
        # Create a Parent Actor in the existing system and verify connectivity
        parent1 = asys.createActor(Parent)
        r = asys.ask(parent1, 'Hello', ask_wait)
        assert 'Hi' == r

        # Create a new ActorSystem, with a new Parent Actor and ensure
        # that both the old and new Actors can still communicate.
        aS = similar_asys(asys, in_convention=False, start_wait=False)
        try:

            parent = aS.createActor(Parent)
            r = aS.ask(parent, 'Hello', ask_wait)
            assert 'Hi' == r

            r = aS.ask(parent, FwdMsg([parent1,parent,parent1]), ask_wait * 10)
            assert [parent1,parent,parent1] == r.pathdone

        finally:
            aS.tell(aS.createActor(SysStopper), 'shut it down')

    def testConnectToStoppingActorSystem(self, asys):
        parent1 = asys.createActor(Parent)
        r = asys.ask(parent1, 'Hello', ask_wait)
        assert 'Hi' == r
        asys.tell(parent1, 'Sleep')  # Parent will prevent shutdown for a little while
        asys.tell(asys.createActor(SysStopper), 'stop system')

        # Access system internals to make singleton "forget" about the
        # current ActorSystem.  This is done so that a new local
        # ActorSystem object can be obtained, but it's check on a
        # system-global admin finds that admin ... which is shutting
        # down.
        ActorSystem.systemBase = None

        aS = similar_asys(asys, in_convention=False, start_wait=False)
        try:
            parent = aS.createActor(Parent)
            # Should never get here...
            r = aS.ask(parent, 'Hello', ask_wait)
            assert 'Hi' == r

        except ActorSystemFailure:
            pass
        except NoCompatibleSystemForActor:
            pass  # this is expected, although it takes a while to get (10s)
        finally:
            pass
            aS.tell(aS.createActor(SysStopper), 'shut it down')
