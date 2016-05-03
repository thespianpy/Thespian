"""Verify DeadLetter handling behavior.

Current behavior is that an Actor may register for DeadLetter
handling.  If it is registered, any message sent to an Actor that is
no longer present will be redirected to the register DeadLetter actor
(in its original form).

On exit of the DeadLetter handling Actor, the system reverts to the
default where dead letters are discarded.

If another Actor registers for DeadLetter handling, the new
registration will supercede the old registration.  The original
handler is not aware of this, and will no longer receive DeadLetters,
even if the new handler de-registers.

Dead letters are handled by the local ActorSystem.  Even if the parent
of an Actor is located in a separate system, the DeadLetter handler is
in the local System.
"""


import unittest
import time
import thespian.test.helpers
from thespian.actors import *
from thespian.test import ActorSystemTestCase

class DLHandler(Actor):
    def receiveMessage(self, msg, sender):
        if msg == 'Start':
            self.handleDeadLetters()
        elif msg == 'Stop':
            self.handleDeadLetters(False)
        elif msg == 'Count':
            self.send(sender, getattr(self, 'numDeadLetters', 0))
        elif isinstance(msg, ActorExitRequest):
            pass
        else:
            # got a dead letter
            self.numDeadLetters = getattr(self, 'numDeadLetters', 0) + 1


class DLParent(Actor):
    def receiveMessage(self, msg, sender):
        if not isinstance(msg, ActorSystemMessage): # or isinstance(msg, DeadEnvelope):
            if not getattr(self, 'dlchild', None):
                self.dlchild = self.createActor(DLHandler)
            if self.dlchild == sender:
                # Upward
                self.send(self.lastSender, msg)
            else:
                # Downward
                self.lastSender = sender
                if msg == 'exit please':
                    self.send(self.dlchild, ActorExitRequest())
                else:
                    self.send(self.dlchild, msg)


class TestASimpleSystem(ActorSystemTestCase):
    testbase='Simple'
    scope='func'

    def checkNewDLCount(self, handlerAddress, oldCount):
        asys = ActorSystem()
        cnt = asys.ask(handlerAddress, 'Count', 0.5)
        retries = 30
        while cnt <= oldCount and retries:
            retries -= 1
            time.sleep(0.025)
            cnt = asys.ask(handlerAddress, 'Count', 0.5)
        self.assertGreater(cnt, oldCount)
        return cnt

    def test01_registerDeadLetter(self):
        handler = ActorSystem().createActor(DLHandler)
        self.assertEqual(0, ActorSystem().ask(handler, 'Count', 0.5))
        ActorSystem().tell(handler, 'Start')
        self.assertEqual(0, ActorSystem().ask(handler, 'Count', 0.5))
        ActorSystem().tell(handler, 'Stop')
        self.assertEqual(0, ActorSystem().ask(handler, 'Count', 0.5))

    def test11_registerDeadLetterSubActor(self):
        handler = ActorSystem().createActor(DLParent)
        self.assertEqual(0, ActorSystem().ask(handler, 'Count', 0.5))
        ActorSystem().tell(handler, 'Start')
        self.assertEqual(0, ActorSystem().ask(handler, 'Count', 0.5))
        ActorSystem().tell(handler, 'Stop')
        self.assertEqual(0, ActorSystem().ask(handler, 'Count', 0.5))

    def test02_GetDeadLetter(self):
        asys = ActorSystem()
        handler = asys.createActor(DLHandler)
        self.assertEqual(0, ActorSystem().ask(handler, 'Count', 0.5))
        asys.tell(handler, 'Start')
        cnt = self.checkNewDLCount(handler, -1)

        pawn = asys.createActor(DLHandler)
        asys.tell(pawn, ActorExitRequest())
        time.sleep(0.025)

        asys.tell(pawn, 'hello')
        cnt = self.checkNewDLCount(handler, cnt)
        asys.tell(pawn, 'hi')
        cnt = self.checkNewDLCount(handler, cnt)

        asys.tell(handler, 'Stop')
        time.sleep(0.025)

        asys.tell(pawn, 'another')
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))
        asys.tell(pawn, 'and another')
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))

    def test12_GetDeadLetterSubActor(self):
        asys = ActorSystem()
        handler = asys.createActor(DLParent)
        self.assertEqual(0, ActorSystem().ask(handler, 'Count', 0.5))
        asys.tell(handler, 'Start')
        cnt = self.checkNewDLCount(handler, -1)

        pawn = asys.createActor(DLParent)
        asys.tell(pawn, 'exit please')
        time.sleep(0.25)

        asys.tell(pawn, 'hello')
        cnt = self.checkNewDLCount(handler, cnt)
        asys.tell(pawn, 'hi')
        cnt = self.checkNewDLCount(handler, cnt)

        asys.tell(handler, 'Stop')
        time.sleep(0.025)

        asys.tell(pawn, 'another')
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))
        asys.tell(pawn, 'and another')
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))

    def test03_DLRegisterOnlyOnce(self):
        asys = ActorSystem()
        handler = asys.createActor(DLHandler)
        self.assertEqual(0, ActorSystem().ask(handler, 'Count', 0.5))
        asys.tell(handler, 'Start')
        cnt = self.checkNewDLCount(handler, -1)

        # Create another actor and shut it down so we can capture its dead letters

        pawn = asys.createActor(DLHandler)
        asys.tell(pawn, ActorExitRequest())
        time.sleep(0.025)

        # Send a couple of messages and verify they are each passed to the dead letter handler

        asys.tell(pawn, 'hello')
        cnt = self.checkNewDLCount(handler, cnt)
        asys.tell(pawn, 'hi')
        cnt = self.checkNewDLCount(handler, cnt)

        # Another start has no effect; remains the dead letter handler.

        asys.tell(handler, 'Start')
        time.sleep(0.02)

        # Send another couple of messages to the dead actor and verify dead letter receipt.

        asys.tell(pawn, 'another')
        cnt = self.checkNewDLCount(handler, cnt)
        asys.tell(pawn, 'and another')
        cnt = self.checkNewDLCount(handler, cnt)

    def test13_DLRegisterOnlyOnce(self):
        asys = ActorSystem()
        handler = asys.createActor(DLParent)
        self.assertEqual(0, ActorSystem().ask(handler, 'Count', 0.5))
        asys.tell(handler, 'Start')
        cnt = self.checkNewDLCount(handler, -1)

        # Create another actor and shut it down so we can capture its dead letters

        pawn = asys.createActor(DLParent)
        asys.tell(pawn, ActorExitRequest())
        time.sleep(0.025)

        # Send a couple of messages and verify they are each passed to the dead letter handler

        asys.tell(pawn, 'hello')
        cnt = self.checkNewDLCount(handler, cnt)
        asys.tell(pawn, 'hi')
        cnt = self.checkNewDLCount(handler, cnt)

        # Another start has no effect; remains the dead letter handler.

        asys.tell(handler, 'Start')
        time.sleep(0.02)

        # Send another couple of messages to the dead actor and verify dead letter receipt.

        asys.tell(pawn, 'another')
        cnt = self.checkNewDLCount(handler, cnt)
        asys.tell(pawn, 'and another')
        cnt = self.checkNewDLCount(handler, cnt)

    def test04_DLMultipleHandlers(self):
        asys = ActorSystem()
        handler = asys.createActor(DLHandler)
        self.assertEqual(0, asys.ask(handler, 'Count', 0.5))
        asys.tell(handler, 'Start')
        cnt = self.checkNewDLCount(handler, -1)

        pawn = asys.createActor(DLHandler)
        asys.tell(pawn, ActorExitRequest())
        time.sleep(0.02)

        asys.tell(pawn, 'hello')
        cnt = self.checkNewDLCount(handler, cnt)
        asys.tell(pawn, 'hi')
        cnt = self.checkNewDLCount(handler, cnt)

        handler2 = asys.createActor(DLHandler)
        asys.tell(handler2, 'Start')
        time.sleep(0.025)

        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))
        self.assertEqual(0, asys.ask(handler2, 'Count', 0.5))
        cnt2 = self.checkNewDLCount(handler2, -1)

        asys.tell(pawn, 'another')
        cnt2 = self.checkNewDLCount(handler2, cnt2)
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))
        asys.tell(pawn, 'and another')
        cnt2 = self.checkNewDLCount(handler2, cnt2)
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))

        asys.tell(handler, 'Stop')  # no effect
        time.sleep(0.025)

        asys.tell(pawn, 'more messages')
        cnt2 = self.checkNewDLCount(handler2, cnt2)
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))

        asys.tell(pawn, 'more messages again')
        cnt2 = self.checkNewDLCount(handler2, cnt2)
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))

        asys.tell(handler2, 'Stop')
        time.sleep(0.025)
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))
        self.assertEqual(cnt2, asys.ask(handler2, 'Count', 0.5))

        asys.tell(pawn, 'more messages repeated')
        time.sleep(0.025)
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))
        self.assertEqual(cnt2, asys.ask(handler2, 'Count', 0.5))

        asys.tell(pawn, 'more messages again repeated')
        time.sleep(0.025)
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))
        self.assertEqual(cnt2, asys.ask(handler2, 'Count', 0.5))

        asys.tell(handler, 'Start')
        time.sleep(0.025)
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))
        self.assertEqual(cnt2, asys.ask(handler2, 'Count', 0.5))

        asys.tell(pawn, 'more messages repeated reprised')
        cnt = self.checkNewDLCount(handler, cnt)
        self.assertEqual(cnt2, asys.ask(handler2, 'Count', 0.5))

        asys.tell(pawn, 'more messages again repeated reprised')
        cnt = self.checkNewDLCount(handler, cnt)
        self.assertEqual(cnt2, asys.ask(handler2, 'Count', 0.5))

    def test14_DLMultipleHandlers(self):
        asys = ActorSystem()
        handler = asys.createActor(DLParent)
        self.assertEqual(0, asys.ask(handler, 'Count', 0.5))
        asys.tell(handler, 'Start')
        cnt = self.checkNewDLCount(handler, -1)

        pawn = asys.createActor(DLParent)
        asys.tell(pawn, ActorExitRequest())
        time.sleep(0.02)

        asys.tell(pawn, 'hello')
        cnt = self.checkNewDLCount(handler, cnt)
        asys.tell(pawn, 'hi')
        cnt = self.checkNewDLCount(handler, cnt)

        handler2 = asys.createActor(DLParent)
        asys.tell(handler2, 'Start')
        time.sleep(0.025)

        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))
        self.assertEqual(0, asys.ask(handler2, 'Count', 0.5))
        cnt2 = self.checkNewDLCount(handler2, -1)

        asys.tell(pawn, 'another')
        cnt2 = self.checkNewDLCount(handler2, cnt2)
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))
        asys.tell(pawn, 'and another')
        cnt2 = self.checkNewDLCount(handler2, cnt2)
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))

        asys.tell(handler, 'Stop')  # no effect
        time.sleep(0.025)

        asys.tell(pawn, 'more messages')
        cnt2 = self.checkNewDLCount(handler2, cnt2)
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))

        asys.tell(pawn, 'more messages again')
        cnt2 = self.checkNewDLCount(handler2, cnt2)
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))

        asys.tell(handler2, 'Stop')
        time.sleep(0.025)
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))
        self.assertEqual(cnt2, asys.ask(handler2, 'Count', 0.5))

        asys.tell(pawn, 'more messages repeated')
        time.sleep(0.025)
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))
        self.assertEqual(cnt2, asys.ask(handler2, 'Count', 0.5))

        asys.tell(pawn, 'more messages again repeated')
        time.sleep(0.025)
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))
        self.assertEqual(cnt2, asys.ask(handler2, 'Count', 0.5))

        asys.tell(handler, 'Start')
        time.sleep(0.025)
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))
        self.assertEqual(cnt2, asys.ask(handler2, 'Count', 0.5))

        asys.tell(pawn, 'more messages repeated reprised')
        cnt = self.checkNewDLCount(handler, cnt)
        self.assertEqual(cnt2, asys.ask(handler2, 'Count', 0.5))

        asys.tell(pawn, 'more messages again repeated reprised')
        cnt = self.checkNewDLCount(handler, cnt)
        self.assertEqual(cnt2, asys.ask(handler2, 'Count', 0.5))

    def test05_DLAutoRemoval(self):
        asys = ActorSystem()
        handler = asys.createActor(DLHandler)
        self.assertEqual(0, asys.ask(handler, 'Count', 0.5))
        asys.tell(handler, 'Start')
        handler2 = asys.createActor(DLHandler)
        asys.tell(handler2, 'Start')
        self.assertEqual(0, asys.ask(handler, 'Count', 0.5))
        self.assertEqual(0, asys.ask(handler2, 'Count', 0.5))

        # Create actor and kill it so messages to it it will be dead-letter routed.

        pawn = asys.createActor(DLHandler)
        asys.tell(pawn, ActorExitRequest())
        time.sleep(0.025)

        # Send a message ane make sure the later dead-letter handler receives it

        cnt = 0
        cnt2 = 0
        asys.tell(pawn, 'hello')
        cnt2 = self.checkNewDLCount(handler2, cnt2)
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))

        # Again, to ensure no round-robining is occurring

        asys.tell(pawn, 'hi')
        cnt2 = self.checkNewDLCount(handler2, cnt2)
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))


        # Now remove dead letter handler; ensure dead letters are dropped

        asys.tell(handler2, ActorExitRequest())
        time.sleep(0.025)
        self.assertEqual(0, asys.ask(handler, 'Count', 0.5))

        asys.tell(pawn, 'another')
        time.sleep(0.025)
        self.assertEqual(0, asys.ask(handler, 'Count', 0.5))

        # Tell first dead letter handler to re-register

        asys.tell(handler, 'Start')
        # n.b. tell or ask might create temporary actor, so can't assume startnum == 0
        cnt = asys.ask(handler, 'Count', 0.5)

        # Verify first dead letter handler is getting dead letters again

        asys.tell(pawn, 'another again')
        cnt = self.checkNewDLCount(handler, cnt)

    def test15_DLAutoRemoval(self):
        asys = ActorSystem()
        handler = asys.createActor(DLParent)
        self.assertEqual(0, asys.ask(handler, 'Count', 0.5))
        asys.tell(handler, 'Start')
        handler2 = asys.createActor(DLParent)
        asys.tell(handler2, 'Start')
        time.sleep(0.15)
        self.assertEqual(0, asys.ask(handler, 'Count', 0.5))
        self.assertEqual(0, asys.ask(handler2, 'Count', 0.5))

        # Create actor and kill it so messages to it it will be dead-letter routed.

        pawn = asys.createActor(DLParent)
        asys.tell(pawn, ActorExitRequest())
        time.sleep(0.025)

        # Send a message and make sure the later dead-letter handler receives it

        cnt = 0
        cnt2 = 0
        asys.tell(pawn, 'hello')
        cnt2 = self.checkNewDLCount(handler2, cnt2)
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))

        # Again, to ensure no round-robining is occurring

        asys.tell(pawn, 'hi')
        cnt2 = self.checkNewDLCount(handler2, cnt2)
        self.assertEqual(cnt, asys.ask(handler, 'Count', 0.5))


        # Now remove dead letter handler; ensure dead letters are dropped

        asys.tell(handler2, ActorExitRequest())
        time.sleep(0.025)
        self.assertEqual(0, asys.ask(handler, 'Count', 0.5))

        asys.tell(pawn, 'another')
        time.sleep(0.025)
        self.assertEqual(0, asys.ask(handler, 'Count', 0.5))

        # Tell first dead letter handler to re-register

        asys.tell(handler, 'Start')
        time.sleep(0.15)
        # n.b. tell or ask might create temporary actor, so can't assume startnum == 0
        cnt = asys.ask(handler, 'Count', 0.5)

        # Verify first dead letter handler is getting dead letters again

        asys.tell(pawn, 'another again')
        cnt = self.checkNewDLCount(handler, cnt)


#KWQ: test multiple actor systems

# UDP does not provide the ability to validate delivery of messages
# (outside of higher-level validation handshakes), so this system base
# cannot support Dead Lettering (as documented).
#
#class TestMultiprocUDPSystem(TestASimpleSystem):
#    testbase='MultiprocUDP'
#    def setUp(self):
#        self.setSystemBase('multiprocUDPBase')
#        super(TestMultiprocUDPSystem, self).setUp()

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

