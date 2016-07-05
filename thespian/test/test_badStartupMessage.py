from thespian.actors import *
from thespian.test import *

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


class TestFuncStartupMessageFail(object):

    def test_fail_on_startup_message(self, asys):
        asys.systemUpdate('setProcessingLimit', 10)  # Ensure simpleSystemBase doesn't loop forever
        asys.systemUpdate('dupLogToFile', '/tmp/bsm.log')  # Ensure simpleSystemBase doesn't loop forever
        parent = asys.createActor(Parent)
        assert 'Hi' == asys.ask(parent, 'Hello', 3)
        asys.tell(parent, 'Make Child')
        assert 'Hi' == asys.ask(parent, 'Hello', 3)
        import time
        time.sleep(1)  # Use to be (20) to allow analysis
        assert 'Hi' == asys.ask(parent, 'Hello', 3)
