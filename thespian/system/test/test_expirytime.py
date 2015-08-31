import unittest
import thespian.test.helpers
from thespian.system.utilis import ExpiryTime
from datetime import datetime, timedelta
from time import sleep


class TestExpiryTime(unittest.TestCase):
    scope="unit"

    def testNoneExpired(self):
        et = ExpiryTime(None)
        self.assertFalse(et.expired())

    def testZeroExpired(self):
        et = ExpiryTime(timedelta(seconds=0))
        self.assertTrue(et.expired())

    def testNonZeroExpired(self):
        et = ExpiryTime(timedelta(milliseconds=10))
        self.assertFalse(et.expired())
        sleep(et.remainingSeconds())
        self.assertTrue(et.expired())

    def testNoneRemaining(self):
        et = ExpiryTime(None)
        self.assertIsNone(et.remaining())

    def testZeroRemaining(self):
        et = ExpiryTime(timedelta(seconds=0))
        self.assertEqual(timedelta(days=0), et.remaining())

    def testNonZeroRemaining(self):
        et = ExpiryTime(timedelta(milliseconds=10))
        self.assertTrue(timedelta(days=0) < et.remaining())
        self.assertTrue(timedelta(milliseconds=11) > et.remaining())
        sleep(et.remainingSeconds())
        self.assertEqual(timedelta(days=0), et.remaining())

    def testNoneRemainingSeconds(self):
        et = ExpiryTime(None)
        self.assertIsNone(et.remainingSeconds())

    def testZeroRemainingSeconds(self):
        et = ExpiryTime(timedelta(microseconds=0))
        self.assertEqual(0.0, et.remainingSeconds())

    def testNonZeroRemainingSeconds(self):
        et = ExpiryTime(timedelta(milliseconds=10))
        self.assertTrue(0.0 < et.remainingSeconds())
        self.assertTrue(0.0101 > et.remainingSeconds())
        sleep(et.remainingSeconds())
        self.assertEqual(0.0, et.remainingSeconds())

    def testNoneRemainingExplicitForever(self):
        et = ExpiryTime(None)
        self.assertEqual(5, et.remaining(5))

    def testNoneRemainingSecondsExplicitForever(self):
        et = ExpiryTime(None)
        self.assertEqual(9, et.remainingSeconds(9))

    def testNoneStr(self):
        et = ExpiryTime(None)
        self.assertEqual('Forever', str(et))

    def testZeroStr(self):
        et = ExpiryTime(timedelta(hours=0))
        self.assertTrue(str(et).startswith('Expired_for_0:00:00'))

    def testNonZeroStr(self):
        et = ExpiryTime(timedelta(milliseconds=10))
        self.assertTrue(str(et).startswith('Expires_in_0:00:00.0'))
        sleep(et.remainingSeconds())
        self.assertTrue(str(et).startswith('Expired_for_0:00:00'))

    def testNoneEquality(self):
        et1 = ExpiryTime(None)
        et2 = ExpiryTime(None)
        self.assertEqual(et1, et2)
        self.assertEqual(et2, et1)

    def testNoneToExpiredComparison(self):
        et1 = ExpiryTime(None)
        et2 = ExpiryTime(timedelta(minutes=0))
        self.assertNotEqual(et1, et2)
        self.assertNotEqual(et2, et1)

    def testNoneToUnExpiredComparison(self):
        et1 = ExpiryTime(None)
        et2 = ExpiryTime(timedelta(milliseconds=10))
        self.assertNotEqual(et1, et2)
        self.assertNotEqual(et2, et1)
        sleep(et2.remainingSeconds())
        self.assertNotEqual(et1, et2)
        self.assertNotEqual(et2, et1)

    def testExpiredToExpiredComparison(self):
        et1 = ExpiryTime(timedelta(microseconds=0))
        sleep(0.001)
        et2 = ExpiryTime(timedelta(minutes=0))
        self.assertEqual(et1, et2)
        self.assertEqual(et2, et1)

    def testExpiredToUnExpiredComparison(self):
        et1 = ExpiryTime(timedelta(microseconds=0))
        et2 = ExpiryTime(timedelta(milliseconds=10))
        self.assertNotEqual(et1, et2)
        self.assertNotEqual(et2, et1)
        sleep(et2.remainingSeconds())
        self.assertEqual(et1, et2)
        self.assertEqual(et2, et1)

    def testUnExpiredToUnExpiredComparison(self):
        et1 = ExpiryTime(timedelta(milliseconds=15))
        et2 = ExpiryTime(timedelta(milliseconds=10))
        self.assertNotEqual(et1, et2)
        self.assertNotEqual(et2, et1)
        sleep(et2.remainingSeconds())
        self.assertNotEqual(et1, et2)
        self.assertNotEqual(et2, et1)
        sleep(et1.remainingSeconds())
        self.assertEqual(et1, et2)
        self.assertEqual(et2, et1)

    def testNoneInEquality(self):
        et1 = ExpiryTime(None)
        et2 = ExpiryTime(None)
        self.assertFalse(et1 != et2)
        self.assertFalse(et2 != et1)

    def testNoneGreaterThanNone(self):
        et1 = ExpiryTime(None)
        et2 = ExpiryTime(None)
        # None == forever, so it is greater than anything, although equal to itself
        self.assertFalse(et1 > et2)
        self.assertFalse(et2 > et1)

    def testNoneComparedToZero(self):
        et1 = ExpiryTime(None)
        et2 = ExpiryTime(timedelta(days=0))
        # None == forever, so it is greater than anything, although equal to itself
        self.assertGreater(et1, et2)
        self.assertLess(et2, et1)
        self.assertTrue(et1 >= et2)
        self.assertTrue(et2 <= et1)

    def testNoneComparedToNonZero(self):
        et1 = ExpiryTime(None)
        et2 = ExpiryTime(timedelta(milliseconds=10))
        # None == forever, so it is greater than anything, although equal to itself
        self.assertGreater(et1, et2)
        self.assertLess(et2, et1)
        self.assertTrue(et1 > et2)
        self.assertTrue(et2 < et1)
        sleep(et2.remainingSeconds())
        self.assertGreater(et1, et2)
        self.assertLess(et2, et1)
        self.assertTrue(et1 > et2)
        self.assertTrue(et2 < et1)

    def testNoneLessThanNone(self):
        et1 = ExpiryTime(None)
        et2 = ExpiryTime(None)
        self.assertFalse(et1 < et2)
        self.assertFalse(et2 < et1)

    def testNoneLessThanEqualNone(self):
        et1 = ExpiryTime(None)
        et2 = ExpiryTime(None)
        self.assertTrue(et1 >= et2)
        self.assertTrue(et2 >= et1)

    def testNoneGreaterThanEqualNone(self):
        et1 = ExpiryTime(None)
        et2 = ExpiryTime(None)
        self.assertTrue(et1 <= et2)
        self.assertTrue(et2 <= et1)

    def testNoneIsFalse(self):
        et = ExpiryTime(None)
        self.assertFalse(et)
        self.assertFalse(bool(et))

    def testZeroIsTrue(self):
        et = ExpiryTime(timedelta(minutes=0))
        self.assertTrue(et)
        self.assertTrue(bool(et))

    def testNonZeroIsFalse(self):
        et = ExpiryTime(timedelta(milliseconds=10))
        self.assertFalse(et)
        self.assertFalse(bool(et))
        sleep(et.remainingSeconds())
        self.assertTrue(et)
        self.assertTrue(bool(et))
