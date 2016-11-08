from datetime import datetime, timedelta
from time import sleep

from thespian.system.ratelimit import RateThrottle
from thespian.system.timing import timePeriodSeconds



def timeDiffuSec(t1, t2):
    td = t2 - t1
    return td.seconds * 1000 * 1000 + td.microseconds


def send_at_rate(max_rate, actual_rate, total_count):  # return timedelta
    rt = RateThrottle(max_rate)
    deltaT = (1.0 / actual_rate) * 1000 * 1000  # microseconds
    startT = datetime.now()
    for each in range(int(total_count)):
        tick = datetime.now()
        rt.eventRatePause()
        tock = datetime.now()
        elapsed = timeDiffuSec(tick, tock)
        if elapsed < deltaT:
            sleep( (deltaT - elapsed) / 1000.0 / 1000.0 )
    finalT = datetime.now()
    return finalT - startT


class TestUnitRateLimit(object):

    def testModerate__expected_duration_is_20_seconds(self):
        cnt = 100
        tt = send_at_rate(10, 5, cnt)
        print('send_at_rate(10, 5, %s) --> %s'%(cnt, str(tt)))
        actRate = cnt / timePeriodSeconds(tt)
        assert 10 > actRate

    def testNearMax__expected_duration_is_11_seconds(self):
        cnt = 100
        tt = send_at_rate(10, 9, cnt)
        print('send_at_rate(10, 9, %s) --> %s'%(cnt, str(tt)))
        actRate = cnt / timePeriodSeconds(tt)
        assert 10 > actRate

    def testOverMax__expected_duration_is_21_seconds(self):
        # The rate limiter allows bursts.. the bigger cnt is the
        # closer to the actual limit the result will be, but the
        # longer the test will take.
        cnt = 300
        tt = send_at_rate(10, 100, cnt)
        print('send_at_rate(10, 100, %s) --> %s'%(cnt, str(tt)))
        actRate = cnt / timePeriodSeconds(tt)
        assert 15 > actRate  # add a little buffer for overage... just a little

    def testSlow__expected_duration_is_20_seconds(self):
        cnt = 10
        tt = send_at_rate(10, 0.5, cnt)
        print('send_at_rate(10, 0.5, %s) --> %s'%(cnt, str(tt)))
        actRate = cnt / timePeriodSeconds(tt)
        assert 10 > actRate


if __name__ == "__main__":
    for ar in [5, 9, 0.2, 100]:
        #cnt = 10 * ar     # this way all return 10 s unless rate limited
        cnt = 1000 if ar > 10 else (50 if ar >= 1 else 5)   # this way see time taken for const # of inputs
        tt = send_at_rate(10, ar, cnt)
        actRate = cnt / timePeriodSeconds(tt)
        print('send_at_rate(10, %s, %s) --> %s, rate=%s'%(str(ar), str(cnt), str(tt), actRate))

