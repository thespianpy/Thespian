import time
from datetime import timedelta


###
### Time Management
###


# Use the function currentTime in this module to get a number representing the current time. The time
# will be denoted in seconds but the return value is not suitable for wall clock time measurement
# but only for measuring time intervals.
#
# For Python 3.3 and later, we will use the CPU performance counter (see PEP-418) which is a monotonic
# clock. For older Python versions we fall back to time.time() which is less accurate and also not
# monotonic.
try:
    currentTime = time.perf_counter
except (AttributeError, NameError):
    currentTime = time.time


def timePeriodSeconds(basis):
    if isinstance(basis, timedelta):
        try:
            return basis.total_seconds()
        except AttributeError:
            # Must be Python 2.6... which doesn't have total_seconds yet
            return (basis.days * 24.0 * 60 * 60) + basis.seconds + (basis.microseconds / 1000.0 / 1000)
    elif isinstance(basis, (int, float)):
        # assume the raw value is already in seconds
        return basis
    raise TypeError('Cannot determine time from a %s argument'%str(type(basis)))


def toTimeDeltaOrNone(timespec):
    if timespec is None: return None
    if isinstance(timespec, timedelta): return timespec
    if isinstance(timespec, int): return timedelta(seconds=timespec)
    if isinstance(timespec, float):
        return timedelta(seconds=int(timespec),
                         microseconds = int((timespec - int(timespec)) * 1000 * 1000))
    raise TypeError('Unknown type for timespec: %s'%type(timespec))


class Timer(object):
    """Keeps track of the elapsed time since its creation or the last call
       to reset().
    """
    def __init__(self):
        self._fromTime = currentTime()
    def reset(self):
        """Restarts the timer."""
        self._fromTime = currentTime()
    def view(self, curtime):
        return TimerView(self._fromTime, curtime)


class TimerView(object):
    def __init__(self, tstart, curtime):
        self._start = tstart
        self._now   = curtime
    def elapsed(self):
        ":return: a timedelta object representing the elapsed time."
        return timedelta(seconds=(currentTime() - self._start))
    def elapsed_seconds(self):
        ":return: A float representing the elapsed time in seconds."
        return timePeriodSeconds(self.elapsed())
    def __str__(self):
        return 'Started_on_' + str(self._start)
    def __eq__(self, o):
        return abs(self._start - o._start) < timedelta(microseconds=1)
    def __lt__(self, o):
        return self._start < o._start
    def __gt__(self, o):
        return not self.__lt__(o)
    def __le__(self, o): return self.__eq__(o) or self.__lt__(o)
    def __ge__(self, o): return self.__eq__(o) or self.__gt__(o)
    def __ne__(self, o): return not self.__eq__(o)
    def __nonzero__(self): return self.elapsedSeconds()


class ExpirationTimer(object):
    """Keeps track of a duration relative to an original time and
       indicates whether that duration has expired or how much time is
       left before it expires.

       May also be initialized with a duration of None, indicating
       that it should never timeout and that `remaining()` should
       return the forever value (defaulting to None).
    """
    def __init__(self, duration=None):
        self.duration = duration
        if duration is None:
            self._time_to_quit = None
        elif isinstance(duration, timedelta):
            self._time_to_quit = currentTime() + timePeriodSeconds(duration)
        else:
            self._time_to_quit = currentTime() + duration
    def view(self, curtime = None):
        return ExpirationTimerView(self.duration, self._time_to_quit, curtime)
    def __enter__(self, curtime = None):
        return self.view(curtime)
    def __exit__(self, exc_type, exc_value, traceback):
        pass
    def __str__(self):
        if self._time_to_quit is None: return 'Forever'
        ct = currentTime()
        if self.view(ct).expired():
            return 'Expired_for_%s' % \
                timedelta(seconds=ct - self._time_to_quit)
        return 'Expires_in_' + str(self.view(ct).remaining())
    def __eq__(self, o):
        # If compared to an arbitrary object that cannot reasonably be
        # considered to be a time, simply return False.  Suppress all
        # exceptions.
        ct = currentTime()
        try:
            if isinstance(o, timedelta):
                o = ExpirationTimer(o)
            if self._time_to_quit == o._time_to_quit: return True
            if self._time_to_quit == None or o._time_to_quit == None: return False
            if self.view(ct).expired() and o.view(ct).expired(): return True
            return abs(self._time_to_quit - o._time_to_quit) < \
                timePeriodSeconds(seconds=timedelta(microseconds=1))
        except Exception:
            return False
    @staticmethod
    def _normalize_o(o):
        if isinstance(o, timedelta):
            return o
        o._time_to_quit # Cause exception if an invalid type
        return o
    def __lt__(self, o):
        # Explicitly allow comparison to None as equivalent to an expired timer.
        # Comparison to arbitrary values/objects throws an exception.
        if o is None:
            return False
        o = self._normalize_o(o)
        try:
            if self._time_to_quit is None and o._time_to_quit is None:
                return False
        except Exception:
            pass
        if self._time_to_quit is None:
            return False
        if o._time_to_quit is None:
            return True
        return self._time_to_quit < o._time_to_quit
    def __gt__(self, o):
        # Explicitly allow comparison to None as equivalent to an expired timer.
        # Comparison to arbitrary values/objects throws an exception.
        if o is None:
            return True
        o = self._normalize_o(o)
        try:
            if self._time_to_quit is None and o._time_to_quit is None:
                return False
        except Exception:
            pass
        return not self.__lt__(o)
    def __le__(self, o): return self.__eq__(o) or self.__lt__(o)
    def __ge__(self, o): return self.__eq__(o) or self.__gt__(o)
    def __ne__(self, o): return not self.__eq__(o)
    def __bool__(self): return self.view().expired()
    def __nonzero__(self): return self.view().expired()


class ExpirationTimerView(ExpirationTimer):
    """Snapshot of an ExpirationTimer status relative to a specific point
       in time.  This allows multiple statements to be executed on a
       stable perspective of the ExpirationTimer.
    """
    def __init__(self, duration, time_to_quit, current_time):
        self.duration = duration
        self._time_to_quit = time_to_quit
        self._current_time = current_time or currentTime()
    def view(self):
        return self
    def expired(self):
        "Returns true if the indicated duration has passed since this was created."
        return False if self._time_to_quit is None \
            else (self._current_time >= self._time_to_quit)
    def remaining(self, forever=None):
        """Returns a timedelta of remaining time until expiration, or 0 if the
           duration has already expired.  Returns forever if no timeout."""
        return forever if self._time_to_quit is None else \
            timedelta(seconds=self.remainingSeconds())
    def remainingSeconds(self, forever=None):
        """Similar to `remaining()`, but returns an floating point value of the
           number of remaining seconds instead of returning a
           timedelta object.
        """
        return forever if self._time_to_quit is None else \
            max(self._time_to_quit - self._current_time, 0)
    def __str__(self):
        if self._time_to_quit is None: return 'Forever'
        if self.expired():
            return 'Expired_for_%s' % \
                timedelta(seconds=self._current_time - self._time_to_quit)
        return 'Expires_in_' + str(self.remaining())
    def __eq__(self, o):
        # If compared to an arbitrary object that cannot reasonably be
        # considered to be a time, simply return False.  Suppress all
        # exceptions.
        try:
            if isinstance(o, timedelta):
                o = ExpirationTimer(o)
            if self._time_to_quit == o._time_to_quit: return True
            if self._time_to_quit == None or o._time_to_quit == None: return False
            if self.expired() and o.view(self._current_time).expired(): return True
            return abs(self._time_to_quit - o._time_to_quit) < \
                timePeriodSeconds(seconds=timedelta(microseconds=1))
        except Exception:
            return False
    def __bool__(self): return self.expired()
    def __nonzero__(self): return self.expired()


def unexpired(timer):
    """A helper function for implementing a common pattern.  Callers can
       simply perform:

           for timeview in unexpired(myTimer):
              ...
              x = timeview.remaining()
              ...
    """
    while True:
        rval = timer.view()
        if rval.expired():
            return
        yield rval
