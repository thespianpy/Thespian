from datetime import datetime, timedelta
import logging
import os
import tempfile
from thespian.actors import InvalidActorSpecification


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



###
### Logging
###

# Default/current logging controls
_thesplog_control_settings = (
    logging.INFO,
    False,
    os.getenv('THESPLOG_FILE_MAXSIZE', 50 * 1024) # 50KB by default
)

# Usually logging would be directed to /var/log, but that is often not
# user-writeable, so $TMPDIR (or /tmp) is used by default; setting
# THESPLOG_FILE is encouraged.  The below variables are set on first
# use to allow direction to be set post load of this file.
_thesplog_file = None
_thesplog_old_file = None


def thesplog_control(baseLevel=logging.DEBUG, useLogging=True, tmpFileMaxSize=0):
    """Specifies the logging performed by thesplog().

       The first parameter specifies the baseLevel for logging output;
       any log messages whose severity is lower than this level are
       not logged (DEBUG is the lowest level, CRITICAL is the highest
       level).

       The useLogging parameter specifies whether messages are to be
       logged via the normal logging in Thespian that Actor logging
       will also use.  The default is True.

       The tmpFileMaxSize, if > 10KB, specifies the maximum size of
       the thespian.log file to write logging output to.  A value
       of 0 (or < 10KB) means that no logging to the thespian.log
       file will be performed.  Note that the actual footprint is
       double this size: when this size is reached, the existing
       ${TMPDIR}/thespian.log file is renamed to ${TMPDIR}/thespian.log.old
       (removing any existing file with that target name) and then a
       new empty thespian.log file is created for subsequent
       logging.
    """

    global _thesplog_control_settings
    _thesplog_control_settings = (baseLevel, useLogging, tmpFileMaxSize)


def thesplog(msg, *args, **kw):
    global _thesplog_control_settings
    if kw.get('level', logging.INFO) >= _thesplog_control_settings[0]:
        if int(_thesplog_control_settings[2]) >= 10 * 1024:
            levelstr = lambda l: { logging.DEBUG: 'dbg',
                                   logging.INFO:  'I',
                                   logging.WARNING: 'Warn',
                                   logging.ERROR:   'ERR',
                                   logging.CRITICAL: 'CRIT' }.get(l, '??')
            global _thesplog_file, _thesplog_old_file
            if not _thesplog_file:
                _thesplog_file = os.getenv('THESPLOG_FILE',
                                           os.path.join(os.getenv('TMPDIR',
                                                                  tempfile.gettempdir()),
                                                        'thespian.log'))
                _thesplog_old_file = _thesplog_file + '.old'
            try:
                if os.stat(_thesplog_file).st_size > int(_thesplog_control_settings[2]):
                    # Tricky: a multiprocess system might enter here
                    # with multiple processes.  The single line append
                    # write below is atomic. Rename should be as well,
                    # but don't try anything more than that.
                    os.rename(_thesplog_file, _thesplog_old_file)
            except OSError:
                # The logfile didn't exist or another process already
                # rotated it.  Move along.
                pass
            try:
                with open(_thesplog_file, 'a') as lf:
                    lf.write('%s p%s %-4s %s\n'%(str(datetime.now()), os.getpid(),
                                                  levelstr(kw.get('level', logging.INFO)), str(msg%args)))
            except Exception:
                # It should not be fatal if there was an error writing
                # to the logfile (e.g. the disk was full)
                pass
        # The Thespian environment uses its own transport to forward
        # logging messages.  This can be dangerous if the transport itself
        # generates logging output because this can lead to a never-ending
        # logging storm.  The primary=True keyword argument can be used
        # with thesplog to request the item to be logged to standard logging;
        # this argument should be used carefully to ensure the logging
        # storm scenario is not triggered.
        if _thesplog_control_settings[1] and kw.get('primary', False):
            oldSettings = _thesplog_control_settings
            _thesplog_control_settings = oldSettings[0], False, oldSettings[1]
            logging.getLogger('Thespian.System').log(
                kw.get('level', logging.INFO), msg, *args,
                exc_info=kw.get('exc_info', False),
                extra=kw.get('extra', None))
            _thesplog_control_settings = oldSettings



###
### Common Actor operations
###

def checkActorCapabilities(actorClass, capabilities=None, requirements=None,
                           sourceHashLoader=None):
    actorClass = actualActorClass(actorClass, sourceHashLoader)
    if not hasattr(actorClass, "actorSystemCapabilityCheck"): return True
    try:
        return actorClass.actorSystemCapabilityCheck(capabilities or {},
                                                     requirements or {})
    except Exception as ex:
        # The Actor may have a bug in their implementation of
        # actorSystemCapabilityCheck, but perhaps there is another
        # ActorSystem for which the Actor's actorSystemCapabilityCheck
        # will succeed, so this is a soft failure.
        return False


def isStr(var):
    # Needed for Python2 and Python 3 compatibility
    if isinstance(var, str): return True
    try:
        return isinstance(var, unicode)
    except NameError:
        return False


def actualActorClass(actorClass, sourceHashLoader=None):
    # the actorClass can either be a class object already or
    # it can be a string.  If it's the latter, get the actual
    # class object corresponding to the string.
    if isStr(actorClass):
        # actorClass is a module-qualified object reference
        # (e.g. "thespian.test.testLoadSource.BarActor').
        classModule, adot, className = actorClass.rpartition('.')
        if not classModule:
            # Caller passed an unqualified name string.  The name is
            # presumably in the same file context as the caller, and
            # for some systemBases (those that share the same process)
            # it might be possible to walk up the call frames and find
            # the right context, but that is not universally possible
            # (esp. for multi-process configurations), so this is
            # *always* disallowed.
            raise InvalidActorSpecification(actorClass)
        else:
            try:
                import importlib
            except ImportError:
                import thespian.importlib as importlib # KWQ?
            if sourceHashLoader:
                actorClass = sourceHashLoader(classModule, className)
            else:
                m = importlib.import_module(classModule)
                actorClass = getattr(m, className)
    return actorClass


###
### Functional operations
###

import functools

try:
    foldl = reduce
except NameError:
    foldl = functools.reduce


def partition(testPred, inpList):
    """Splits a list into two lists: the first list contains the elements
       that pass the testPred (i.e. testPred(Element) is True), and the
       second list contains elements that do not pass the testPred."""
    appLeft  = lambda ll, e: (ll[0]+[e], ll[1])
    appRight = lambda ll, e: (ll[0],     ll[1]+[e])
    appendLeftOrRight = lambda ll, e: (appLeft if testPred(e) else appRight)(ll, e)
    return foldl(appendLeftOrRight, inpList, ([],[]))



def fmap(func, obj):
    if isinstance(obj, tuple):
        return tuple(map(functools.partial(fmap, func), obj))
    iterableitems =  isinstance(obj, list)
    if not iterableitems:
        try:
            iterableitems = isinstance(obj, (filter, map, zip))
        except TypeError:
            pass
    if iterableitems:
        return map(functools.partial(fmap, func), obj)
    if hasattr(obj, 'fmap'):
        return obj.fmap(func)
    return func(obj)



###
### Useful object for managing Stats
###

class StatsManager(object):

    def __init__(self):
        self._kv = {}

    def inc(self, kw):
        if kw not in self._kv:
            self._kv[kw] = 1
        else:
            self._kv[kw] += 1

    def copyToStatusResponse(self, response):
        for kw in self._kv:
            response.addKeyVal(kw, self._kv[kw])



###
### Miscellaneous
###

def setProcName(name, actorAddr):
    try: from setproctitle import setproctitle
    #This library not required, but its presence will make
    #actor names and addresses available in the process list.
    except: pass
    else: setproctitle('%s %s'%(name, str(actorAddr)))
