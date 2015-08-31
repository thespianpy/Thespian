from datetime import datetime, timedelta
from time import sleep as delay
from thespian.system.utilis import timePeriodSeconds

class RateThrottle(object):
    """This object is used to provide rate throttling for activities.  It
       should be called each time an event is to occur and provides
       varying delays to ensure the rate is throttled appropriately.

       The rate throttling is performed on an accelerating basis:
       initial operations are not throttled, and the amount of
       throttling depends on the level of previous traffic.  The goal
       is to perform no throttling unless and until the number of
       events reaches a level of concern.
    """

    def __init__(self, maximumRate):
        "Specify the maximum rate for events to occur (in # events/sec)"
        self._maxRate = maximumRate
        self._runningCount = 0

    def __str__(self):
        return 'Rate limit: %s messages/sec (currently %s with %s ticks)'%(self._maxRate,
                                                                           getattr(self, '_curRate', 'low'),
                                                                           self._runningCount)

    def eventRatePause(self):
        """This is the main method that should be called each time an event is
           to occur.  It will block internally until it is time for
           the next event to occur.
        """
        if not hasattr(self, '_curRate'):
            self._runningCount += 1

            # runningCount is the total # since the last time it was
            # zeroed, so it's not just wthin the last second, but it does
            # provide a threshold above which the actual rate should be
            # observed and possibly throttled.  This will begin to take an
            # interest in the actual rate when the runningCount reaches an
            # arbitrary 70% of the maximum rate.
            if self._runningCount < (self._maxRate * 0.70):
                return

            self._curRate = 0
            self._timeMark = datetime.now()
            self._goodMarks = 0
            return

        self._curRate += 1
        newT = datetime.now()
        deltaT = newT - self._timeMark

        if deltaT < timedelta(seconds=1):
            return

        rate = self._curRate / timePeriodSeconds(deltaT)

        if rate > self._maxRate:
            # Slow down a little
            delay(0.1)
            self._goodMarks = 0
            return

        self._goodMarks += 1
        if self._goodMarks > self._maxRate:
            delattr(self, '_curRate')
            self._runningCount = 0
            return
