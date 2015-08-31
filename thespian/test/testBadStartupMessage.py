import unittest
import thespian.test.helpers
from thespian.actors import *
from thespian.test import ActorSystemTestCase

"""This test is for a nasty corner case:

   * If a child actor receives a "startup" message from its parent, and
   * the parent sends that startup message each time the child dies (ChildActorExited), and
   * the startup message causes the child actor to die

This tends to cause an infinite loop (especially with the simple system base).

Specific notes for simple system base:

   - The "startup" message is never PoisonMessage'd because the
     current message goes to the enwd of the queue each time and is
     superceded by the newly generated message, so the message queue
     has unconstrained growth of startup messages, each with just a
     single failure.

Specific notes for multiprocess system base:

   - Doesn't manifest as a recursive loop in the current process, but
     the delay in the test is associated with high cpu utilization as
     the Parent and Child actors keep passing messages back and forth
     and restarting.

"""

try:
    skip = unittest.skip
except AttributeError:
    def skip(mssg):
        #raise unittest.SkipTest("Test %s skipped"%test.__doc__)
        #SkipTest does not exist in python 2.6 :(
        print("SKIPPED TEST: %s", mssg)
        return lambda s: True


class Parent(Actor):
    def receiveMessage(self, msg, sender):
        if msg == 'Make Child':
            child = self.createActor(Child)
            self.send(child, 'Hello')
        elif msg == 'Hello':
            self.send(sender, 'Hi')


class Child(Actor):
    def receiveMessage(self, msg, sender):
        if msg == 'Hello':
            raise AttributeError('I am Grumpy!')


class TestASimpleSystem(ActorSystemTestCase):
    testbase='Simple'
    scope='func'

    @skip
    def test_fail_on_startup_message(self):
        aS = ActorSystem()
        aS.systemUpdate('setProcessingLimit', 10)  # Ensure simpleSystemBase doesn't loop forever
        aS.systemUpdate('dupLogToFile', '/tmp/bsm.log')  # Ensure simpleSystemBase doesn't loop forever
        parent = aS.createActor(Parent)
        self.assertEqual('Hi', aS.ask(parent, 'Hello', 3))
        aS.tell(parent, 'Make Child')
        self.assertEqual('Hi', aS.ask(parent, 'Hello', 3))
        import time
        time.sleep(20)
        self.assertEqual('Hi', aS.ask(parent, 'Hello', 3))
        self.assertEqual(2+2, 5)
