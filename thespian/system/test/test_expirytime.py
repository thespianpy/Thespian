import pytest
from thespian.system.timing import ExpirationTimer, currentTime
from datetime import datetime, timedelta
from time import sleep


class TestUnitExpirationTimer(object):

    def testNoneExpired(self):
        et = ExpirationTimer(None)
        assert not et.view().expired()

    def testZeroExpired(self):
        et = ExpirationTimer(timedelta(seconds=0))
        assert et.view().expired()

    def testNonZeroExpired(self):
        et = ExpirationTimer(timedelta(milliseconds=10))
        assert not et.view().expired()
        sleep(et.view().remainingSeconds())
        with et as c:
            assert c.expired()

    def testNoneRemaining(self):
        et = ExpirationTimer(None)
        assert et.view().remaining() is None

    def testZeroRemaining(self):
        et = ExpirationTimer(timedelta(seconds=0))
        assert timedelta(days=0) == et.view().remaining()

    def testNonZeroRemaining(self):
        et = ExpirationTimer(timedelta(milliseconds=10))
        ct = currentTime()
        assert timedelta(days=0) < et.view(ct).remaining()
        assert timedelta(milliseconds=11) > et.view(ct).remaining()
        sleep(et.view().remainingSeconds())
        assert timedelta(days=0) == et.view().remaining()

    def testNoneRemainingSeconds(self):
        et = ExpirationTimer(None)
        assert et.view().remainingSeconds() is None

    def testZeroRemainingSeconds(self):
        et = ExpirationTimer(timedelta(microseconds=0))
        assert 0.0 == et.view().remainingSeconds()

    def testNonZeroRemainingSeconds(self):
        et = ExpirationTimer(timedelta(milliseconds=10))
        with et as c:
            assert 0.0 < c.remainingSeconds()
            assert 0.0101 > c.remainingSeconds()
        sleep(et.view().remainingSeconds())
        assert 0.0 == et.view().remainingSeconds()

    def testNoneRemainingExplicitForever(self):
        et = ExpirationTimer(None)
        assert 5 == et.view().remaining(5)

    def testNoneRemainingSecondsExplicitForever(self):
        et = ExpirationTimer(None)
        assert 9 == et.view().remainingSeconds(9)

    def testNoneStr(self):
        et = ExpirationTimer(None)
        assert 'Forever' == str(et)

    def testZeroStr(self):
        et = ExpirationTimer(timedelta(hours=0))
        assert str(et).startswith('Expired_for_0:00:00')

    def testNonZeroStr(self):
        et = ExpirationTimer(timedelta(milliseconds=10))
        assert str(et).startswith('Expires_in_0:00:00.0')
        sleep(et.view().remainingSeconds())
        assert str(et).startswith('Expired_for_0:00:00')

    def testArbitraryEquality(self):
        et1 = ExpirationTimer(None)
        et2 = ExpirationTimer(0)
        et3 = ExpirationTimer(10)
        assert not et1 == None
        assert not et2 == None
        assert not et3 == None

        assert et1 != None
        assert et2 != None
        assert et3 != None

        assert not et1 == 0
        assert not et2 == 0
        assert not et3 == 0

        assert et1 != 0
        assert et2 != 0
        assert et3 != 0

        assert not et1 == 'hi'
        assert not et2 == 'hi'
        assert not et3 == 'hi'

        assert et1 != 'hi'
        assert et2 != 'hi'
        assert et3 != 'hi'

        assert not et1 == object()
        assert not et2 == object()
        assert not et3 == object()

        assert et1 != object()
        assert et2 != object()
        assert et3 != object()

    def testNoneEquality(self):
        et1 = ExpirationTimer(None)
        et2 = ExpirationTimer(None)
        assert et1 == et2
        assert et2 == et1

    def testNoneToExpiredComparison(self):
        et1 = ExpirationTimer(None)
        et2 = ExpirationTimer(timedelta(minutes=0))
        assert et1 != et2
        assert et2 != et1

    def testNoneToUnExpiredComparison(self):
        et1 = ExpirationTimer(None)
        et2 = ExpirationTimer(timedelta(milliseconds=10))
        assert et1 != et2
        assert et2 != et1
        sleep(et2.view().remainingSeconds())
        assert et1 != et2
        assert et2 != et1

    def testExpiredToExpiredComparison(self):
        et1 = ExpirationTimer(timedelta(microseconds=0))
        sleep(0.001)
        et2 = ExpirationTimer(timedelta(minutes=0))
        assert et1 == et2
        assert et2 == et1

    def testExpiredToUnExpiredComparison(self):
        et1 = ExpirationTimer(timedelta(microseconds=0))
        et2 = ExpirationTimer(timedelta(milliseconds=10))
        assert et1 != et2
        assert et2 != et1
        sleep(et2.view().remainingSeconds())
        assert et1 == et2
        assert et2 == et1

    def testUnExpiredToUnExpiredComparison(self):
        et1 = ExpirationTimer(timedelta(milliseconds=15))
        et2 = ExpirationTimer(timedelta(milliseconds=10))
        assert et1 != et2
        assert et2 != et1
        sleep(et2.view().remainingSeconds())
        print(str(et1), str(et2))
        # The following will fail if an extra 5ms delay has occurred
        assert et1 != et2
        assert et2 != et1
        sleep(et1.view().remainingSeconds())
        assert et1 == et2
        assert et2 == et1

    def testNoneInEquality(self):
        et1 = ExpirationTimer(None)
        et2 = ExpirationTimer(None)
        assert not et1 != et2
        assert not et2 != et1

    def testNoneGreaterThanNone(self):
        et1 = ExpirationTimer(None)
        et2 = ExpirationTimer(None)
        # None == forever, so it is greater than anything, although equal to itself
        assert not et1 > et2
        assert not et2 > et1

    def testNoneComparedToZero(self):
        et1 = ExpirationTimer(None)
        et2 = ExpirationTimer(timedelta(days=0))
        # None == forever, so it is greater than anything, although equal to itself
        assert et1 > et2
        assert et2 < et1
        assert et1 >= et2
        assert et2 <= et1

    def testNoneComparedToNonZero(self):
        et1 = ExpirationTimer(None)
        et2 = ExpirationTimer(timedelta(milliseconds=10))
        # None == forever, so it is greater than anything, although equal to itself
        assert et1 > et2
        assert et2 < et1
        assert et1 > et2
        assert et2 < et1
        sleep(et2.view().remainingSeconds())
        assert et1 > et2
        assert et2 < et1
        assert et1 > et2
        assert et2 < et1

    def testLessThanNone(self):
        et1 = ExpirationTimer(None)
        et2 = ExpirationTimer(0)
        et3 = ExpirationTimer(10)
        assert not et1 < None
        assert not et2 < None
        assert not et3 < None

    def testLessThanArbitrary(self):
        et1 = ExpirationTimer(None)
        et2 = ExpirationTimer(0)
        et3 = ExpirationTimer(10)

        with pytest.raises(AttributeError):
            assert not et1 < 0
        with pytest.raises(AttributeError):
            assert not et2 < 0
        with pytest.raises(AttributeError):
            assert not et3 < 0

        with pytest.raises(AttributeError):
            assert not et1 < 'hi'
        with pytest.raises(AttributeError):
            assert not et2 < 'hi'
        with pytest.raises(AttributeError):
            assert not et3 < 'hi'

        with pytest.raises(AttributeError):
            assert not et1 < object()
        with pytest.raises(AttributeError):
            assert not et2 < object()
        with pytest.raises(AttributeError):
            assert not et3 < object()

    def testNoneLessThanNone(self):
        et1 = ExpirationTimer(None)
        et2 = ExpirationTimer(None)
        assert not et1 < et2
        assert not et2 < et1

    def testNoneLessThanEqualNone(self):
        et1 = ExpirationTimer(None)
        et2 = ExpirationTimer(None)
        assert et1 >= et2
        assert et2 >= et1

    def testGreaterThanArbitrary(self):
        et1 = ExpirationTimer(None)
        et2 = ExpirationTimer(0)
        et3 = ExpirationTimer(10)

        with pytest.raises(AttributeError):
            assert not et1 > 0
        with pytest.raises(AttributeError):
            assert not et2 > 0
        with pytest.raises(AttributeError):
            assert not et3 > 0

        with pytest.raises(AttributeError):
            assert not et1 > 'hi'
        with pytest.raises(AttributeError):
            assert not et2 > 'hi'
        with pytest.raises(AttributeError):
            assert not et3 > 'hi'

        with pytest.raises(AttributeError):
            assert not et1 > object()
        with pytest.raises(AttributeError):
            assert not et2 > object()
        with pytest.raises(AttributeError):
            assert not et3 > object()

    def testNoneGreaterThanEqualNone(self):
        et1 = ExpirationTimer(None)
        et2 = ExpirationTimer(None)
        assert et1 <= et2
        assert et2 <= et1

    def testNoneIsFalse(self):
        et = ExpirationTimer(None)
        assert not et
        assert not bool(et)

    def testZeroIsTrue(self):
        et = ExpirationTimer(timedelta(minutes=0))
        assert et
        assert bool(et)

    def testNonZeroIsFalse(self):
        et = ExpirationTimer(timedelta(milliseconds=10))
        assert not et
        assert not bool(et)
        sleep(et.view().remainingSeconds())
        assert et
        assert bool(et)
