import time

from thespian.system.timing import ExpirationTimer


def test_expiration_timer():
    timer = ExpirationTimer(duration=1.0)
    time.sleep(0.2)

    with timer as t:
        assert t.expired() == False
        assert 0.7 <= t.remainingSeconds() <= 0.9
    assert timer.view().expired() == False
    assert 0.7 <= timer.view().remainingSeconds() <= 0.9

    time.sleep(1.0)

    with timer as t:
        assert t.expired() == True
        assert t.remainingSeconds() == 0.0

def test_expiration_timer_None_period():
    timer = ExpirationTimer(None)
    with timer as t:
        assert t.expired() == False
    assert timer.view().expired() == False

    time.sleep(0.2)

    with timer as t:
        assert t.expired() == False
        assert t.remainingSeconds() == None
    assert timer.view().expired() == False
    assert timer.view().remainingSeconds() == None

def test_expiration_timer_comparison():
    # Timers will compare equal if they do not differ by more than 60
    # microseconds.

    timer1 = ExpirationTimer(duration=1.0)
    timer5 = ExpirationTimer(duration=1.0)

    timer2 = ExpirationTimer(duration=0.9)
    timer3 = ExpirationTimer(duration=0)
    timer4 = ExpirationTimer(None)

    timer6 = ExpirationTimer(duration=10.0)
    timer7 = ExpirationTimer(duration=0)
    timer8 = ExpirationTimer(None)

    assert timer1 > timer2
    assert timer1 > timer3
    assert timer1 < timer4

    assert timer2 < timer1
    assert timer3 < timer1
    assert timer4 > timer1

    assert timer4 > timer2
    assert timer4 > timer3
    assert timer3 < timer4
    assert timer2 > timer3

    assert timer3 < timer2
    assert timer2 > timer3

    # timer5 = ExpirationTimer(duration=1.0)
    # timer6 = ExpirationTimer(duration=10.0)
    # timer7 = ExpirationTimer(duration=0)
    # timer8 = ExpirationTimer(None)

    print('timer5=',timer5,'timer1=',timer1)
    assert timer5 == timer1
    assert timer6 != timer1

    assert timer7 == timer3
    assert timer7 != timer4
    assert timer7 != timer1

    assert timer4 == timer8
    assert not(timer4 > timer8)
    assert not(timer4 < timer8)

    # After expiration, all timers are equal, unless they are forever

    time.sleep(1.1)

    assert timer1 == timer2
    assert timer1 == timer3
    assert timer1 != timer4
    assert timer1 == timer5
    assert timer1 == timer7

    assert timer2 == timer1
    assert timer3 == timer1
    assert timer4 != timer1
    assert timer5 == timer1
    assert timer7 == timer1

