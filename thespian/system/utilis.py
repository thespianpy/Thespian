from datetime import datetime, timedelta
import logging
import os
import tempfile
from thespian.actors import InvalidActorSpecification
import inspect
import warnings


###
### Configuration
###


def str_to_timedelta(strval):
    "timedelta"
    [mins, secs] = list(map(int, strval.split(':')))
    return timedelta(minutes=mins, seconds=secs)


def getenvdef(env_name, converter, defval):
    """Returns the converted representation of the specified environment
       variable or the default value if the environment variable is
       not set or is not convertible to an integer value.
    """
    strval = os.getenv(env_name)
    if strval:
        try:
            return converter(strval)
        except Exception:
            thesplog("Invalid '%s' %s value, using %s", env_name,
                     (converter.__doc__ or "generic").split('\n')[0],
                     defval,
                     level=logging.WARNING);
    return defval


###
### Logging
###

# recognized logging level names for the Thespian internal log.
_name_to_level = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET
}

def str_to_loglevel(strval):
    "logging level"
    return _name_to_level.get(strval)

# Default/current logging controls
_thesplog_control_settings = (
    getenvdef('THESPLOG_THRESHOLD', str_to_loglevel, logging.WARNING),
    False,  # is primary=True on thesplog directed to standard logging [dangerous]?
    getenvdef('THESPLOG_FILE_MAXSIZE', int, 50 * 1024) # 50KB by default
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
        if _thesplog_control_settings[2] >= 10 * 1024:
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
                if os.stat(_thesplog_file).st_size > _thesplog_control_settings[2]:
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
                import thespian.local_importlib as importlib
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
    foldl = reduce    # type: ignore
except NameError:
    foldl = functools.reduce


def _append(iterable, value):
    iterable.append(value)
    return iterable


def join(iterable_of_iterables):
    return foldl(lambda a, b: a + b, iterable_of_iterables, [])


def partition(testPred, inp_iterable, output_type=list):
    """Splits an iterable (e.g. list) into a tuple of two lists (or other
       output_type): the first output iterable contains the elements
       that pass the testPred (i.e. testPred(Element) is True), and
       the second output iterable contains elements that do not pass
       the testPred.
    """
    appLeft  = lambda ll, e: (_append(ll[0], e), ll[1])
    appRight = lambda ll, e: (ll[0], _append(ll[1], e))
    appendLeftOrRight = lambda ll, e: (appLeft if testPred(e) else appRight)(ll, e)
    return foldl(appendLeftOrRight, inp_iterable, (output_type(), output_type()))



def fmap(func, obj):
    if isinstance(obj, tuple):
        return tuple(map(functools.partial(fmap, func), obj))
    iterableitems =  isinstance(obj, (list, dict))
    if not iterableitems:
        try:
            iterableitems = isinstance(obj, (filter, map, zip, range))
        except TypeError:
            # Python2 doesn't have objects like the above.  The
            # corresponding operations just result in lists which is
            # already covered.
            pass
    if iterableitems:
        if hasattr(obj, 'items'):
            return dict(map(functools.partial(fmap, func), obj.items()))
        return list(map(functools.partial(fmap, func), obj))
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


class AssocList(object):
    def __init__(self):
        self._qa = []  # (addr, val)
    def find(self, addr):
        for each in self._qa:
            if each[0] == addr:
                return each[1]
        return None
    def add(self, addr, val):
        self._qa = [(A,V) for (A,V) in self._qa if A != addr] + [(addr,val)]
    def rmv(self, addr):
        self._qa = [(A,V) for (A,V) in self._qa if A != addr]
    def rmv_value(self, val):
        self._qa = [(A,V) for (A,V) in self._qa if V != val]
    def values(self):
        return [V for (A,V) in self._qa]
    def items(self):
        return self._qa
    def fmap(self, func):
        map(func, self._qa)
    def __len__(self):
        return len(self._qa)


class withPossibleInitArgs(object):
    """Allows the creation of an object, passing arguments to the
       __init__() method only if those arguments are specified by
       name.

       Example:

          class Foo(object):
              def __init__(self, bar):  ...

          foo = withPossibleInitArgs(bar=1, cow='moo').create(Foo)

       will construct a Foo object by pass a bar value of 1 to the
       __init__ method.  If the __init__ method required an argument
       of "baz", the construction would fail because the
       withPossibleInitArgs set of arguments did not include a "baz"
       argument.

    """

    def __init__(self, **kw):
        self.kwargs = kw

    def create(self, klass):
        """Creates an instance of the specified class, passing any of the
           initial arguments that are valid arguments for the __init__
           method.
        """

        try:
            initsig = [P.name
                       for P in inspect.signature(klass.__init__).parameters]
        except (ValueError, NameError, AttributeError):
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('error', category=DeprecationWarning)
                    initsig = inspect.getargspec(klass.__init__).args
            except (ValueError, NameError, AttributeError, DeprecationWarning):
                try:
                    initsig = inspect.getfullargspec(klass.__init__).args
                except (ValueError, NameError, AttributeError):
                    # Default to just initializing with no arguments
                    print('defaulting')
                    return klass()
        return klass(**{ k: self.kwargs[k] for k in initsig if k in self.kwargs })
