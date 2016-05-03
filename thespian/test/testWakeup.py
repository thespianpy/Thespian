"""Verify wakeupAfter behavior.

The wakeupAfter call can be used by an Actor to request a
WakeupMessage after a specified time period.  Multiple wakeupAfter
calls can be pending; they cannot be cancelled (although they are
aborted if the Actor is killed).
"""

import unittest
from datetime import datetime, timedelta
import time
import thespian.test.helpers
from thespian.actors import *
from thespian.test import TestSystem

wakeupAfterPeriod = timedelta(seconds=0.65)
sleepLongerThanWakeup = lambda: time.sleep(0.7)
sleepPartOfWakeupPeriod = lambda: time.sleep(0.1)


class RetryActor(Actor):
    def __init__(self):
        self._numWakeups = 0
    def receiveMessage(self, msg, sender):
        if "check" == msg:
            self.wakeupAfter(wakeupAfterPeriod)
        elif isinstance(msg, WakeupMessage):
            self._numWakeups += 1
        elif "awoken?" == msg:
            self.send(sender, self._numWakeups)


class TestASimpleSystem(unittest.TestCase):
    testbase='Simple'
    scope='func'

    sysbase = 'simpleSystemBase'
    portbase = 17600

    def test_oneWakeup(self):
        with TestSystem(self.sysbase, {'Admin Port': self.portbase+1}) as aS:
            waiter = aS.createActor(RetryActor)
            self.assertEqual(aS.ask(waiter, 'awoken?', 1), 0)
            self.assertEqual(aS.ask(waiter, 'awoken?', 1), 0)

            aS.tell(waiter, 'check')
            # Next assert will fail if it takes more than the wakeupPeriod
            # to run after the previous statement.
            self.assertEqual(aS.ask(waiter, 'awoken?', 1), 0)

            sleepLongerThanWakeup()

            self.assertEqual(aS.ask(waiter, 'awoken?', 1), 1)


    def test_threeWakeupsInSequence(self):
        with TestSystem(self.sysbase, {'Admin Port': self.portbase+2}) as aS:
            waiter = aS.createActor(RetryActor)
            self.assertEqual(aS.ask(waiter, 'awoken?', 1), 0)

            aS.tell(waiter, 'check')
            # Next assert will fail if it takes more than the wakeupPeriod
            # to run after the previous statement.
            self.assertEqual(aS.ask(waiter, 'awoken?', 1), 0)

            sleepLongerThanWakeup()
            self.assertEqual(aS.ask(waiter, 'awoken?', 1), 1)

            aS.tell(waiter, 'check')
            # ditto above
            self.assertEqual(aS.ask(waiter, 'awoken?', 1), 1)

            sleepLongerThanWakeup()
            aS.tell(waiter, 'check')
            self.assertEqual(aS.ask(waiter, 'awoken?', 1), 2)

            sleepLongerThanWakeup()
            self.assertEqual(aS.ask(waiter, 'awoken?', 1), 3)


    def test_multipleWakeupsPending(self):
        with TestSystem(self.sysbase, {'Admin Port': self.portbase+3}) as aS:
            waiter = aS.createActor(RetryActor)
            self.assertEqual(aS.ask(waiter, 'awoken?', 1), 0)

            aS.tell(waiter, 'check')
            aS.tell(waiter, 'check')
            # Next assert will fail if it takes more than the wakeupPeriod
            # to run after the previous statement.
            self.assertEqual(aS.ask(waiter, 'awoken?', 1), 0)

            sleepLongerThanWakeup()
            self.assertEqual(aS.ask(waiter, 'awoken?', 1), 2)

            aS.tell(waiter, 'check')
            sleepPartOfWakeupPeriod()
            aS.tell(waiter, 'check')
            sleepPartOfWakeupPeriod()
            aS.tell(waiter, 'check')

            sleepLongerThanWakeup()
            self.assertEqual(aS.ask(waiter, 'awoken?', 1), 5)


    def test_exitWithWakeupsPending(self):
        with TestSystem(self.sysbase, {'Admin Port': self.portbase+4}) as aS:
            waiter = aS.createActor(RetryActor)
            self.assertEqual(aS.ask(waiter, 'awoken?', 1), 0)
            aS.tell(waiter, 'check')
            sleepPartOfWakeupPeriod()
            aS.tell(waiter, ActorExitRequest())
            self.assertTrue(True)  # ensure above doesn't throw exception


class TestMultiprocUDPSystem(TestASimpleSystem):
    testbase='MultiprocUDP'
    sysbase = 'multiprocUDPBase'
    portbase = 17610

class TestMultiprocTCPSystem(TestASimpleSystem):
    testbase='MultiprocTCP'
    sysbase = 'multiprocTCPBase'
    portbase = 17620

class TestMultiprocQueueSystem(TestASimpleSystem):
    testbase='MultiprocQueue'
    sysbase = 'multiprocQueueBase'
    portbase = 17630
