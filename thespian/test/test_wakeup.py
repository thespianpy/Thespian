"""Verify wakeupAfter behavior.

The wakeupAfter call can be used by an Actor to request a
WakeupMessage after a specified time period.  Multiple wakeupAfter
calls can be pending; they cannot be cancelled (although they are
aborted if the Actor is killed).
"""

from thespian.test import *
from datetime import datetime, timedelta
import time
from thespian.actors import *


# Note that this test is highly subjective based on the scheduling
# sequence, message delivery latency, and responsiveness of the
# current system.  Significantly larger numbers here will increase the
# stability of the test at the expense of test duration.  The
# Multiprocess Queue library seems to have longer internal delays.
wakeupAfterPeriod = timedelta(seconds=0.065)
sleepLongerThanWakeup = lambda sys: time.sleep(2.1
                                               if sys.base_name == 'multiprocQueueBase'
                                               else 0.6)
sleepPartOfWakeupPeriod = lambda: time.sleep(0.005)



class RetryActor(Actor):
    def __init__(self):
        self._numWakeups = {}
    def receiveMessage(self, msg, sender):
        if "check" == msg or "awoken?" == msg:
            self.dispatch(sender, (msg, None))
        elif isinstance(msg, WakeupMessage):
            if msg.payload not in self._numWakeups:
                self._numWakeups[msg.payload] = 0
            self._numWakeups[msg.payload] += 1
        elif isinstance(msg, tuple):
            self.dispatch(sender, msg)
    def dispatch(self, sender, msg):
        cmd, payload = msg
        if cmd == "check":
            self.wakeupAfter(wakeupAfterPeriod, payload)
        elif cmd == "awoken?":
            self.send(sender, self._numWakeups.get(payload, 0))

class TestFuncWakeup(object):

    def test_oneWakeup(self, asys):
        waiter = asys.createActor(RetryActor)
        assert asys.ask(waiter, 'awoken?', 1) == 0
        assert asys.ask(waiter, 'awoken?', 1) == 0

        asys.tell(waiter, 'check')
        # Next assert will fail if it takes more than the wakeupPeriod
        # to run after the previous statement.
        assert asys.ask(waiter, 'awoken?', 1) == 0

        sleepLongerThanWakeup(asys)

        assert asys.ask(waiter, 'awoken?', 1) == 1

    def test_twoWakeupsDifferentPayloads(self, asys):
        waiter = asys.createActor(RetryActor)
        assert asys.ask(waiter, 'awoken?', 1) == 0
        assert asys.ask(waiter, ('awoken?', 'payload_1'), 1) == 0
        assert asys.ask(waiter, ('awoken?', 'payload_2'), 1) == 0

        asys.tell(waiter, ('check', 'payload_1'))
        # Next assert will fail if it takes more than the wakeupPeriod
        # to run after the previous statement.
        assert asys.ask(waiter, ('awoken?', 'payload_1'), 1) == 0

        sleepLongerThanWakeup(asys)

        assert asys.ask(waiter, 'awoken?', 1) == 0
        assert asys.ask(waiter, ('awoken?', 'payload_1'), 1) == 1
        assert asys.ask(waiter, ('awoken?', 'payload_2'), 1) == 0

        asys.tell(waiter, ('check', 'payload_2'))

        sleepLongerThanWakeup(asys)

        assert asys.ask(waiter, 'awoken?', 1) == 0
        assert asys.ask(waiter, ('awoken?', 'payload_1'), 1) == 1
        assert asys.ask(waiter, ('awoken?', 'payload_2'), 1) == 1


    def test_threeWakeupsDifferentPayloads(self, asys):
        waiter = asys.createActor(RetryActor)
        assert asys.ask(waiter, 'awoken?', 1) == 0
        assert asys.ask(waiter, ('awoken?', 'payload_1'), 1) == 0
        assert asys.ask(waiter, ('awoken?', ''), 1) == 0

        asys.tell(waiter, ('check', 'payload_1'))
        # Next assert will fail if it takes more than the wakeupPeriod
        # to run after the previous statement.
        assert asys.ask(waiter, ('awoken?', 'payload_1'), 1) == 0

        sleepLongerThanWakeup(asys)

        assert asys.ask(waiter, 'awoken?', 1) == 0
        assert asys.ask(waiter, ('awoken?', 'payload_1'), 1) == 1
        assert asys.ask(waiter, ('awoken?', ''), 1) == 0

        asys.tell(waiter, ('check', ''))
        sleepPartOfWakeupPeriod()
        asys.tell(waiter, 'check')

        sleepLongerThanWakeup(asys)

        assert asys.ask(waiter, 'awoken?', 1) == 1
        assert asys.ask(waiter, ('awoken?', 'payload_1'), 1) == 1
        assert asys.ask(waiter, ('awoken?', ''), 1) == 1


    def test_threeWakeupsInSequence(self, asys):
        waiter = asys.createActor(RetryActor)
        assert asys.ask(waiter, 'awoken?', 1) == 0

        asys.tell(waiter, 'check')
        # Next assert will fail if it takes more than the wakeupPeriod
        # to run after the previous statement.
        assert asys.ask(waiter, 'awoken?', 1) == 0

        sleepLongerThanWakeup(asys)
        assert asys.ask(waiter, 'awoken?', 1) == 1

        asys.tell(waiter, 'check')
        # ditto above
        assert asys.ask(waiter, 'awoken?', 1) == 1

        sleepLongerThanWakeup(asys)
        asys.tell(waiter, 'check')
        assert asys.ask(waiter, 'awoken?', 1) == 2

        sleepLongerThanWakeup(asys)
        assert asys.ask(waiter, 'awoken?', 1) == 3


    def test_multipleWakeupsPending(self, asys):
        waiter = asys.createActor(RetryActor)
        assert asys.ask(waiter, 'awoken?', 1) == 0

        asys.tell(waiter, 'check')
        asys.tell(waiter, 'check')
        # Next assert will fail if it takes more than the wakeupPeriod
        # to run after the previous statement.
        assert asys.ask(waiter, 'awoken?', 1) == 0

        sleepLongerThanWakeup(asys)
        assert asys.ask(waiter, 'awoken?', 1) == 2

        asys.tell(waiter, 'check')
        sleepPartOfWakeupPeriod()
        asys.tell(waiter, 'check')
        sleepPartOfWakeupPeriod()
        asys.tell(waiter, 'check')

        sleepLongerThanWakeup(asys)
        awoken = asys.ask(waiter, 'awoken?', 1)
        assert awoken == 5


    def test_exitWithWakeupsPending(self, asys):
        waiter = asys.createActor(RetryActor)
        assert asys.ask(waiter, 'awoken?', 1) == 0
        asys.tell(waiter, 'check')
        sleepPartOfWakeupPeriod()
        asys.tell(waiter, ActorExitRequest())
        assert True  # ensure above doesn't throw exception
