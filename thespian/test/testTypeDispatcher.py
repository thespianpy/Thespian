import unittest
import logging
import time, datetime
import thespian.test.helpers
from thespian.actors import *
from thespian.test import ActorSystemTestCase

class Message1(object):
    def __init__(self):
        self.m1 = "m1"

class Message2(Message1):
    def __init__(self):
        super(Message2, self).__init__()
        self.m2 = "m2"

class Message3(object): pass
class Message4(object): pass

class StrangeMessage1(Message3): pass
class StrangeMessage2(object): pass


class Bottom(ActorTypeDispatcher):
    def receiveMsg_str(self, msg, sender):
        self.send(sender, "got a string")

    def receiveMsg_Message1(self, msg, sender):
        self.send(sender, "got " + msg.m1)

class Middle(Bottom):
    def receiveMsg_Message2(self, msg, sender):
        self.send(sender, "middle got " + msg.m2)
        return self.SUPER

    def receiveMsg_Message3(self, msg, sender):
        self.send(sender, "middle got #3")

    def receiveUnrecognizedMessage(self, msg, sender):
        self.send(sender, "didn't recognize: %s"%str(type(msg)))

class Top(Middle):
    def receiveMsg_Message3(self, msg, sender):
        self.send(sender, "top got msg3")
        return self.SUPER

    def receiveMsg_Message2(self, msg, sender):
        self.send(sender, "top got " + msg.m2)

    def receiveMsg_str(self, msg, sender):
        self.send(sender, 'top got "'+msg+'"')

    def receiveMsg_int(self, msg, sender):
        self.send(sender, msg * 2)


class TestASimpleSystem(ActorSystemTestCase):
    testbase='Simple'
    scope='func'

    def setUp(self):
        super(TestASimpleSystem, self).setUp()
        self.bottom = ActorSystem().createActor(Bottom)
        self.middle = ActorSystem().createActor(Middle)
        self.top    = ActorSystem().createActor(Top)

    def tearDown(self):
        for A in [self.bottom, self.middle, self.top]:
            ActorSystem().tell(A, ActorExitRequest())

    def verifyExpectedResponses(self, target, requestMsg, expected):
        resp = ActorSystem().ask(target, requestMsg, 0.1)
        if expected == None:
            self.assertIsNone(resp)
        else:
            while resp:
                self.assertIn(resp, expected)
                del expected[expected.index(resp)]
                resp = ActorSystem().listen(0.1)
            self.assertEqual(len(expected), 0)

    # --- Bottom --------------------------------------------------

    def testBottomString(self):
        self.verifyExpectedResponses(self.bottom, "Are you there?",
                                     ["got a string"])

    def testBottomInt(self):
        self.verifyExpectedResponses(self.bottom, 9, None)

    def testBottomMsg1(self):
        self.verifyExpectedResponses(self.bottom, Message1(), ["got m1"])

    def testBottomMsg2(self):
        self.verifyExpectedResponses(self.bottom, Message2(), ["got m1"])

    def testBottomMsg3(self):
        self.verifyExpectedResponses(self.bottom, Message3(), None)

    # --- Middle --------------------------------------------------

    def testMiddleString(self):
        self.verifyExpectedResponses(self.middle, "Are you there?",
                                     ["got a string"])

    def testMiddleInt(self):
        import sys
        tname = 'class' if sys.version_info >= (3,0,0) else 'type'
        self.verifyExpectedResponses(self.middle, 9, ["didn't recognize: <%s 'int'>"%tname])

    def testMiddleMsg1(self):
        self.verifyExpectedResponses(self.middle, Message1(), ["got m1"])

    def testMiddleMsg2(self):
        expected = ["middle got m2", "got m1"]
        resp = ActorSystem().ask(self.middle, Message2(), 0.1)
        while resp:
            self.assertIn(resp, expected)
            del expected[expected.index(resp)]
            resp = ActorSystem().listen(0.1)
        self.assertEqual(len(expected), 0)

    def testMiddleMsg3(self):
        self.verifyExpectedResponses(self.middle, Message3(), ["middle got #3"])

    # --- Top --------------------------------------------------

    def testTopString(self):
        self.verifyExpectedResponses(self.top, "Are you there?",
                                     ['top got "Are you there?"'])

    def testTopInt(self):
        self.verifyExpectedResponses(self.top, 9, [18])

    def testTopMsg1(self):
        self.verifyExpectedResponses(self.top, Message1(), ["got m1"])

    def testTopMsg2(self):
        expected = ["top got m2"]
        resp = ActorSystem().ask(self.top, Message2(), 0.1)
        while resp:
            self.assertIn(resp, expected)
            del expected[expected.index(resp)]
            resp = ActorSystem().listen(0.1)
        self.assertEqual(len(expected), 0)

    def testTopMsg3(self):
        self.verifyExpectedResponses(self.top, Message3(), ["top got msg3",
                                                            "middle got #3"])

    # --- Unrecognized fallbacks --------------------------------

    def testBottomStrangeSubclass(self):
        self.verifyExpectedResponses(self.bottom, StrangeMessage1(), None)

    def testBottomUnrecognized(self):
        self.verifyExpectedResponses(self.bottom, StrangeMessage2(), None)

    def testMiddleStrangeSubclass(self):
        self.verifyExpectedResponses(self.middle, StrangeMessage1(), ["middle got #3"])

    def testMiddleUnrecognized(self):
        self.verifyExpectedResponses(self.middle, StrangeMessage2(), ["didn't recognize: <class 'thespian.test.testTypeDispatcher.StrangeMessage2'>"])

    def testTopStrangeSubclass(self):
        self.verifyExpectedResponses(self.top, StrangeMessage1(), ["top got msg3",
                                                                   "middle got #3"])

    def testTopUnrecognized(self):
        self.verifyExpectedResponses(self.top, StrangeMessage2(),
                                     ["didn't recognize: <class "
                                      "'thespian.test.testTypeDispatcher.StrangeMessage2'>"])

