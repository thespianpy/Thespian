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
