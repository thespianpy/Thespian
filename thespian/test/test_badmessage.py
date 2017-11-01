from thespian.actors import *
from thespian.test import *
import time
from datetime import timedelta
import sys


max_response_delay = timedelta(seconds=1.0)


class BadMessage(object):
    def __init__(self, val):
        self.val = val
    def __str__(self):
        return 'Using an invalid member: ' + str(self.this_does_not_exist)

class BadMessage2(object):     # ok to str() this one
    def __init__(self, val):
        self.val = val
    def __str__(self):
        return 'BadMsg2=' + str(self.val)


class MyActor(Actor):
    def __init__(self):
        self.count = 0
    def receiveMessage(self, msg, sender):
        self.count += 1
        if isinstance(msg, (BadMessage, BadMessage2)) and (self.count & 1):
            raise Exception('Got a BadMessage: ' + str(msg))
        if not isinstance(msg, ActorSystemMessage):
            self.send(sender, str(msg))


def test01_actorWorks(asys):
    mya = asys.createActor(MyActor)
    r = asys.ask(mya, 123, max_response_delay)
    assert r == '123'

def test02_alwaysBad(asys):
    mya = asys.createActor(MyActor)
    r = asys.ask(mya, BadMessage(135), max_response_delay)
    assert isinstance(r, PoisonMessage)
    assert isinstance(r.poisonMessage, BadMessage)
    assert r.poisonMessage.val == 135


def test03_intermittentlyBad(asys):
    mya = asys.createActor(MyActor)

    # First one should be OK
    r = asys.ask(mya, BadMessage2(987), max_response_delay)
    assert r is not None
    assert '987' in r

    # Second one gets the exception the first time around, but the
    # Actor should be re-instated and the message retried, and it
    # should work the second time, so the failure is undetectable at
    # this level.
    r2 = asys.ask(mya, BadMessage2(654), max_response_delay)
    assert r2 is not None
    assert '654' in r2
