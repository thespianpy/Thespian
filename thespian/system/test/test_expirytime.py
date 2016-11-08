from thespian.system.timing import ExpiryTime
from datetime import datetime, timedelta
from time import sleep


class TestUnitExpiryTime(object):

    def testNoneExpired(self):
        et = ExpiryTime(None)
        assert not et.expired()

    def testZeroExpired(self):
        et = ExpiryTime(timedelta(seconds=0))
        assert et.expired()

    def testNonZeroExpired(self):
        et = ExpiryTime(timedelta(milliseconds=10))
        assert not et.expired()
        sleep(et.remainingSeconds())
        assert et.expired()

    def testNoneRemaining(self):
        et = ExpiryTime(None)
        assert et.remaining() is None

    def testZeroRemaining(self):
        et = ExpiryTime(timedelta(seconds=0))
        assert timedelta(days=0) == et.remaining()

    def testNonZeroRemaining(self):
        et = ExpiryTime(timedelta(milliseconds=10))
        assert timedelta(days=0) < et.remaining()
        assert timedelta(milliseconds=11) > et.remaining()
        sleep(et.remainingSeconds())
        assert timedelta(days=0) == et.remaining()

    def testNoneRemainingSeconds(self):
        et = ExpiryTime(None)
        assert et.remainingSeconds() is None

    def testZeroRemainingSeconds(self):
        et = ExpiryTime(timedelta(microseconds=0))
        assert 0.0 == et.remainingSeconds()

    def testNonZeroRemainingSeconds(self):
        et = ExpiryTime(timedelta(milliseconds=10))
        assert 0.0 < et.remainingSeconds()
        assert 0.0101 > et.remainingSeconds()
        sleep(et.remainingSeconds())
        assert 0.0 == et.remainingSeconds()

    def testNoneRemainingExplicitForever(self):
        et = ExpiryTime(None)
        assert 5 == et.remaining(5)

    def testNoneRemainingSecondsExplicitForever(self):
        et = ExpiryTime(None)
        assert 9 == et.remainingSeconds(9)

    def testNoneStr(self):
        et = ExpiryTime(None)
        assert 'Forever' == str(et)

    def testZeroStr(self):
        et = ExpiryTime(timedelta(hours=0))
        assert str(et).startswith('Expired_for_0:00:00')

    def testNonZeroStr(self):
        et = ExpiryTime(timedelta(milliseconds=10))
        assert str(et).startswith('Expires_in_0:00:00.0')
        sleep(et.remainingSeconds())
        assert str(et).startswith('Expired_for_0:00:00')

    def testNoneEquality(self):
        et1 = ExpiryTime(None)
        et2 = ExpiryTime(None)
        assert et1 == et2
        assert et2 == et1

    def testNoneToExpiredComparison(self):
        et1 = ExpiryTime(None)
        et2 = ExpiryTime(timedelta(minutes=0))
        assert et1 != et2
        assert et2 != et1

    def testNoneToUnExpiredComparison(self):
        et1 = ExpiryTime(None)
        et2 = ExpiryTime(timedelta(milliseconds=10))
        assert et1 != et2
        assert et2 != et1
        sleep(et2.remainingSeconds())
        assert et1 != et2
        assert et2 != et1

    def testExpiredToExpiredComparison(self):
        et1 = ExpiryTime(timedelta(microseconds=0))
        sleep(0.001)
        et2 = ExpiryTime(timedelta(minutes=0))
        assert et1 == et2
        assert et2 == et1

    def testExpiredToUnExpiredComparison(self):
        et1 = ExpiryTime(timedelta(microseconds=0))
        et2 = ExpiryTime(timedelta(milliseconds=10))
        assert et1 != et2
        assert et2 != et1
        sleep(et2.remainingSeconds())
        assert et1 == et2
        assert et2 == et1

    def testUnExpiredToUnExpiredComparison(self):
        et1 = ExpiryTime(timedelta(milliseconds=15))
        et2 = ExpiryTime(timedelta(milliseconds=10))
        assert et1 != et2
        assert et2 != et1
        sleep(et2.remainingSeconds())
        print(str(et1), str(et2))
        assert et1 != et2
        assert et2 != et1
        sleep(et1.remainingSeconds())
        assert et1 == et2
        assert et2 == et1

    def testNoneInEquality(self):
        et1 = ExpiryTime(None)
        et2 = ExpiryTime(None)
        assert not et1 != et2
        assert not et2 != et1

    def testNoneGreaterThanNone(self):
        et1 = ExpiryTime(None)
        et2 = ExpiryTime(None)
        # None == forever, so it is greater than anything, although equal to itself
        assert not et1 > et2
        assert not et2 > et1

    def testNoneComparedToZero(self):
        et1 = ExpiryTime(None)
        et2 = ExpiryTime(timedelta(days=0))
        # None == forever, so it is greater than anything, although equal to itself
        assert et1 > et2
        assert et2 < et1
        assert et1 >= et2
        assert et2 <= et1

    def testNoneComparedToNonZero(self):
        et1 = ExpiryTime(None)
        et2 = ExpiryTime(timedelta(milliseconds=10))
        # None == forever, so it is greater than anything, although equal to itself
        assert et1 > et2
        assert et2 < et1
        assert et1 > et2
        assert et2 < et1
        sleep(et2.remainingSeconds())
        assert et1 > et2
        assert et2 < et1
        assert et1 > et2
        assert et2 < et1

    def testNoneLessThanNone(self):
        et1 = ExpiryTime(None)
        et2 = ExpiryTime(None)
        assert not et1 < et2
        assert not et2 < et1

    def testNoneLessThanEqualNone(self):
        et1 = ExpiryTime(None)
        et2 = ExpiryTime(None)
        assert et1 >= et2
        assert et2 >= et1

    def testNoneGreaterThanEqualNone(self):
        et1 = ExpiryTime(None)
        et2 = ExpiryTime(None)
        assert et1 <= et2
        assert et2 <= et1

    def testNoneIsFalse(self):
        et = ExpiryTime(None)
        assert not et
        assert not bool(et)

    def testZeroIsTrue(self):
        et = ExpiryTime(timedelta(minutes=0))
        assert et
        assert bool(et)

    def testNonZeroIsFalse(self):
        et = ExpiryTime(timedelta(milliseconds=10))
        assert not et
        assert not bool(et)
        sleep(et.remainingSeconds())
        assert et
        assert bool(et)
