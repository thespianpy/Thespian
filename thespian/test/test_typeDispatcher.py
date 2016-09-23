import pytest
import logging
import time, datetime
import re
from thespian.test import *
from thespian.actors import *
from thespian.system.utilis import fmap
from datetime import timedelta


MAX_ASK_DELAY = timedelta(seconds=3)


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


@pytest.fixture
def bmt_actors(request, asys):
    bottom = asys.createActor(Bottom)
    middle = asys.createActor(Middle)
    top    = asys.createActor(Top)
    return asys, bottom, middle, top

@pytest.fixture
def bottom(request, asys):
    return asys.createActor(Bottom)

@pytest.fixture
def middle(request, asys):
    return asys.createActor(Middle)

@pytest.fixture
def top(request, asys):
    return asys.createActor(Top)


class TestFuncTypeDispatching(object):

    def verifyExpectedResponses(self, asys, target, requestMsg, expected):
        resp = asys.ask(target, requestMsg, MAX_ASK_DELAY)
        if expected == None:
            assert resp is None
        else:
            while resp and expected:
                for n,e in enumerate(expected):
                    if hasattr(e, 'search'):
                        if e.search(resp):
                            break
                    else:
                        if e == resp:
                            break
                else:
                    assert False, 'Expected "%s" not found in: %s'%(
                        resp, fmap(str, expected))
                del expected[n]
                resp = asys.listen(0.1)
            assert len(expected) == 0

    # --- Bottom --------------------------------------------------

    def testBottomString(self, asys, bottom):
        self.verifyExpectedResponses(asys, bottom, "Are you there?",
                                     ["got a string"])

    def testBottomInt(self, asys, bottom):
        self.verifyExpectedResponses(asys, bottom, 9, None)

    def testBottomMsg1(self, asys, bottom):
        self.verifyExpectedResponses(asys, bottom, Message1(), ["got m1"])

    def testBottomMsg2(self, asys, bottom):
        self.verifyExpectedResponses(asys, bottom, Message2(), ["got m1"])

    def testBottomMsg3(self, asys, bottom):
        self.verifyExpectedResponses(asys, bottom, Message3(), None)

    # --- Middle --------------------------------------------------

    def testMiddleString(self, asys, middle):
        self.verifyExpectedResponses(asys, middle, "Are you there?",
                                     ["got a string"])

    def testMiddleInt(self, asys, middle):
        import sys
        tname = 'class' if sys.version_info >= (3,0,0) else 'type'
        self.verifyExpectedResponses(
            asys, middle, 9,
            [re.compile("didn't recognize: <%s 'int'(| at 0x[0-9a-f]+)>"%tname)])

    def testMiddleMsg1(self, asys, middle):
        self.verifyExpectedResponses(asys, middle, Message1(), ["got m1"])

    def testMiddleMsg2(self, asys, middle):
        expected = ["middle got m2", "got m1"]
        resp = asys.ask(middle, Message2(), MAX_ASK_DELAY)
        while resp:
            assert resp in expected
            del expected[expected.index(resp)]
            resp = asys.listen(MAX_ASK_DELAY)
        assert len(expected) == 0

    def testMiddleMsg3(self, asys, middle):
        self.verifyExpectedResponses(asys, middle, Message3(), ["middle got #3"])

    # --- Top --------------------------------------------------

    def testTopString(self, asys, top):
        self.verifyExpectedResponses(asys, top, "Are you there?",
                                     ['top got "Are you there?"'])

    def testTopInt(self, asys, top):
        self.verifyExpectedResponses(asys, top, 9, [18])

    def testTopMsg1(self, asys, top):
        self.verifyExpectedResponses(asys, top, Message1(), ["got m1"])

    def testTopMsg2(self, asys, top):
        expected = ["top got m2"]
        resp = asys.ask(top, Message2(), MAX_ASK_DELAY)
        print('resp',resp)
        while resp:
            assert resp in expected
            del expected[expected.index(resp)]
            resp = asys.listen(MAX_ASK_DELAY)
            print('resp',resp)
        assert len(expected) == 0

    def testTopMsg3(self, asys, top):
        self.verifyExpectedResponses(asys, top, Message3(), ["top got msg3",
                                                            "middle got #3"])

    # --- Unrecognized fallbacks --------------------------------

    def testBottomStrangeSubclass(self, asys, bottom):
        self.verifyExpectedResponses(asys, bottom, StrangeMessage1(), None)

    def testBottomUnrecognized(self, asys, bottom):
        self.verifyExpectedResponses(asys, bottom, StrangeMessage2(), None)

    def testMiddleStrangeSubclass(self, asys, middle):
        self.verifyExpectedResponses(asys, middle,
                                     StrangeMessage1(), ["middle got #3"])

    def testMiddleUnrecognized(self, asys, middle):
        self.verifyExpectedResponses(
            asys, middle, StrangeMessage2(),
            [re.compile("didn't recognize: <class "
                        "'thespian.test.test_typeDispatcher.StrangeMessage2'"
                        "(| at 0x[0-9a-f]+)>")])

    def testTopStrangeSubclass(self, asys, top):
        self.verifyExpectedResponses(asys, top, StrangeMessage1(),
                                     ["top got msg3",
                                      "middle got #3"])

    def testTopUnrecognized(self, asys, top):
        self.verifyExpectedResponses(
            asys, top, StrangeMessage2(),
            [re.compile("didn't recognize: <class "
                        "'thespian.test.test_typeDispatcher.StrangeMessage2'"
                        "(| at 0x[0-9a-f]+)>")])

