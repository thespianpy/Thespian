import time

from thespian.system.timing import ExpirationTimer


def test_expiration_timer():
    timer = ExpirationTimer(duration=1.0)
    time.sleep(0.2)
    assert timer.expired() == False
    assert 0.7 <= timer.remainingSeconds() <= 0.9
    time.sleep(1.0)
    assert timer.expired() == True
    assert timer.remainingSeconds() == 0.0
