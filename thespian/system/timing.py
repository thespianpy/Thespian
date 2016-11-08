from datetime import datetime, timedelta


###
### Time Management
###

def timePeriodSeconds(basis, other=None):
    if isinstance(basis, datetime):
        if isinstance(other, datetime):
            return timePeriodSeconds(other - basis)
    if isinstance(basis, timedelta):
        try:
            return basis.total_seconds()
        except AttributeError:
            # Must be Python 2.6... which doesn't have total_seconds yet
            return (basis.days * 24.0 * 60 * 60) + basis.seconds + (basis.microseconds / 1000.0 / 1000)
    raise TypeError('Cannot determine time from a %s argument'%str(type(basis)))


def toTimeDeltaOrNone(timespec):
    if timespec is None: return None
    if isinstance(timespec, timedelta): return timespec
    if isinstance(timespec, int): return timedelta(seconds=timespec)
    if isinstance(timespec, float):
        return timedelta(seconds=int(timespec),
                         microseconds = int((timespec - int(timespec)) * 1000 * 1000))
    raise TypeError('Unknown type for timespec: %s'%type(timespec))


class ExpiryTime(object):
    def __init__(self, duration):
        self._time_to_quit = None if duration is None else (datetime.now() + duration)
    def expired(self):
        return False if self._time_to_quit is None else (datetime.now() >= self._time_to_quit)
    def remaining(self, forever=None):
        return forever if self._time_to_quit is None else \
            (timedelta(seconds=0) if datetime.now() > self._time_to_quit else \
             (self._time_to_quit - datetime.now()))
    def remainingSeconds(self, forever=None):
        return forever if self._time_to_quit is None else \
            (0 if datetime.now() > self._time_to_quit else \
             timePeriodSeconds(self._time_to_quit - datetime.now()))
    def __str__(self):
        if self._time_to_quit is None: return 'Forever'
        if self.expired():
            return 'Expired_for_%s'%(datetime.now() - self._time_to_quit)
        return 'Expires_in_' + str(self.remaining())
    def __eq__(self, o):
        if isinstance(o, timedelta):
            o = ExpiryTime(o)
        if self._time_to_quit == o._time_to_quit: return True
        if self._time_to_quit == None or o._time_to_quit == None: return False
        if self.expired() and o.expired(): return True
        return abs(self._time_to_quit - o._time_to_quit) < timedelta(microseconds=1)
    def __lt__(self, o):
        try:
            if self._time_to_quit is None and o._time_to_quit is None: return False
        except Exception: pass
        if self._time_to_quit is None: return False
        if isinstance(o, timedelta):
            o = ExpiryTime(o)
        if o._time_to_quit is None: return True
        return self._time_to_quit < o._time_to_quit
    def __gt__(self, o):
        try:
            if self._time_to_quit is None and o._time_to_quit is None: return False
        except Exception: pass
        return not self.__lt__(o)
    def __le__(self, o): return self.__eq__(o) or self.__lt__(o)
    def __ge__(self, o): return self.__eq__(o) or self.__gt__(o)
    def __ne__(self, o): return not self.__eq__(o)
    def __bool__(self): return self.expired()
    def __nonzero__(self): return self.expired()
