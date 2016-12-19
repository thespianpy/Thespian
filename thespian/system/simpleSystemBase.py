'''The SimpleSystemBase is not a true concurrency environment, but it
 runs in the context of the current thread and simply queues actor
 sends to call each actor's handler in sequence.  This sytem can be
 used for simple actor environments where no parallelism is achieved,
 but any actor doing long-running or suspend (e.g. i/o) activities
 will pause or halt the entire system.

  * Synchronous message delivery

  * Local system only

  * All execution is in the current thread (actor system only runs
    when .tell() or .ask() is called).

  * createActor() always creates the actor instance immediately before returning.

'''

import logging, string, types, functools
from thespian.actors import *
from thespian.system.utilis import (actualActorClass, partition)
from thespian.system.timing import timePeriodSeconds, toTimeDeltaOrNone, ExpirationTimer
try:
    from logging.config import dictConfig
except ImportError:
    # Old python that doesn't contain this...
    from thespian.system.dictconfig import dictConfig
from thespian.system import isInternalActorSystemMessage
from thespian.system.messages.status import *
from thespian.system.sourceLoader import loadModuleFromHashSource, SourceHashFinder
import time
import traceback


class ActorRef:
    "Used internally to store the actual actor instance and associated information."
    def __init__(self, actorSystem, parentAddr, addr, inst, mySourceHash=None):
        self._system = actorSystem
        self._oldm = parentAddr
        self._addr = addr
        self._inst = inst  # briefly None until set
        self._mySourceHash = mySourceHash
        self._yung = []   # actorAddress of children
        # The number of current children is len(self._yung); the
        # childCounter keeps track of the total number of children
        # this Actor has had in its ENTIRE lifetime (i.e. it is not
        # decremented when children are removed from self._yung.  The
        # childCounter is used when generating the unique ActorAddress
        # for a new child.
        self._childCounter = 0
    @property
    def parent(self): return self._oldm
    @property
    def address(self): return self._addr
    @property
    def instance(self): return self._inst
    @instance.setter
    def instance(self, newInstance): self._inst = newInstance
    @property
    def childCount(self): return self._childCounter

    def addChild(self, childAddr):
        self._yung.append(childAddr)
        self._childCounter = self._childCounter + 1
    def removeChild(self, childAddr):
        self._yung = [c for c in self._yung if c != childAddr]

    def shutdown(self):
        for child in self._yung:
            self._system._systemBase.actor_send(
                self._system._systemBase.actorRegistry[self._system.systemAddress.actorAddressString].address,
                child, ActorExitRequest())

    # Functionality vectoring for the Actor this represents.
    def createActor(self, actorClass, targetActorRequirements, globalName, sourceHash=None):
        return self._system._systemBase.newActor(self._addr, actorClass, self._system,
                                                 targetActorRequirements, globalName,
                                                 sourceHash or self._mySourceHash)

    def actor_send(self, toActorAddr, msg):
        self._system._systemBase.actor_send(self._addr, toActorAddr, msg)

    def wakeupAfter(self, timePeriod):
        self._system._systemBase.wakeupAfter(self._addr, timePeriod)

    def handleDeadLetters(self, address, enable):
        self._system._handleDeadLetters(address, enable)

    def registerSourceAuthority(self, address):
        self._system._systemBase.registerSourceAuthority(address)

    def notifyOnSourceAvailability(self, address, enable):
        self._system._systemBase.registerSourceNotifications(address, enable)

    def updateCapability(self, capabilityName, capabilityValue):
        self._system.updateCapability(capabilityName, capabilityValue)

    def loadActorSource(self, fname):
        return self._system._systemBase.loadActorSource(fname)

    def unloadActorSource(self, sourceHash):
        self._system._systemBase.unloadActorSource(sourceHash)

    def notifyOnSystemRegistrationChanges(self, address, enable):
        pass # ignored: simple systems don't have registration

    def logger(self, name=None):
        return logging.LoggerAdapter(logging.getLogger(name),
                                     {'actorAddress': self._addr})


# ----------------------------------------------------------------------

_nameValid    = string.ascii_letters   # what characters are valid in an ActorAddress
_nameValidLen = len(_nameValid)    # precompute the size for convenience

def _namegen(v):
    if v == 0: return 'a'
    x,y = divmod(v, _nameValidLen)
    return (_namegen(x) if x else '') + _nameValid[y]

def _newAddress(prefix, childCount):
    return ActorAddress(prefix + '~' + _namegen(childCount))

def _newChildAddress(parentRef):
    "Returns a new candidate ActorAddress for a child Actor about to be created."
    # Note that this address is not fixed/reserved until the child
    # is actually created (thereby incrementing the parentRef
    # childCount), so calling this multiple times without creating
    # the child may return the same value."
    return _newAddress(parentRef.address.actorAddressString, parentRef.childCount)


# ----------------------------------------------------------------------

class PendingSend:
    "used internally for marshalling pending send operations"
    def __init__(self, sender, msg, toActor):
        self.sender = sender
        self.toActor = toActor
        self.msg = msg
        self.attempts = 0
    def __str__(self): return 'PendingSend(#%d %s -> %s: %s)'%(self.attempts, self.sender, self.toActor, self.msg)


class BadActor(Actor):   # useable as a "null" Actor which does nothing.
    name = 'BadActor'
    def receiveMessage(self, msg, sender):
        logging.getLogger('Thespian').debug('BadActor discarding message')
        pass  #   Throws away all messages


class External(Actor):
    """Proxy for a requester outside the system.  Messages sent to this
       Actor will be queued and delivered as the result of
       ActorSystem().ask() and ActorSystem().listen() calls."""
    def receiveMessage(self, msg, sender):
        if not hasattr(self, 'responses'): self.responses = []
        self.responses.append(msg)


def actor_base_receive(actorInst, msg, sender):
    logging.getLogger('Thespian').debug('Actor "%s" got message "%s" from "%s"',
                                        actorInst, msg, sender)
    try:
        actorInst.receiveMessage(msg, sender)
    except:
        logging.getLogger('Thespian').warning('Actor "%s" error processing message "%s"',
                                              actorInst, msg, exc_info=True)
        if isinstance(msg, PoisonMessage):
            logging.getLogger('Thespian').warning('Actor "%s" double-draught of poison; discarding',
                                                  actorInst)
        else:
            actorInst.send(sender, PoisonMessage(msg, traceback.format_exc()))


class actorLogFilter(logging.Filter):
    def filter(self, logrecord): return 'actorAddress' in logrecord.__dict__
class notActorLogFilter(logging.Filter):
    def filter(self, logrecord): return 'actorAddress' not in logrecord.__dict__

import sys

defaultLoggingConfig = {
    'version': 1,
    'formatters': {
        'defaultFmt': {
            'format': '%(asctime)s %(levelname)-7s =>  %(message)s  [%(filename)s:%(lineno)s]',
        },
        'actorFmt': {
            'format': '%(asctime)s %(levelname)-7s %(actorAddress)s =>  %(message)s  [%(filename)s:%(lineno)s]',
        },
    },
    'filters': {
        'isActorLog': {'()': actorLogFilter},
        'notActorLog': {'()': notActorLogFilter},
    },
    'handlers': {
        'actorLogHandler': { 'class': 'logging.StreamHandler',
                             'level': 'WARNING',
                             'stream': sys.stderr,
                             'formatter': 'actorFmt',
                             'filters': [ 'isActorLog' ],
                         },
        'regLogHandler': { 'class': 'logging.StreamHandler',
                           'level': 'WARNING',
                           'stream': sys.stderr,
                           'formatter': 'defaultFmt',
                           'filters': [ 'notActorLog' ],
                         },
    },
    'root': { 'handlers': ['actorLogHandler',
                           'regLogHandler',
                       ],
          },
    'disable_existing_loggers': False,
}


class WakeupManager(object):
    def __init__(self):
        # _wakeUps is a list of (targetAddress, ExpirationTimer)
        self._wakeUps = []

    def _pop_expired_wakeups(self):
        exp, self._wakeUps = partition(lambda E: E[1].expired(), self._wakeUps)
        return exp

    def _next_wakeup(self):
        "Returns the ExpirationTimer for the next wakeup to occur"
        return min([T for A,T in self._wakeUps]) if self._wakeUps else None

    def _add_wakeup(self, from_actor, time_period):
        self._wakeUps.append( (from_actor, ExpirationTimer(time_period)) )

    def add_wakeups_to_status(self, statusmsg):
        statusmsg.addWakeups(self._wakeUps)
        return statusmsg



class ActorSystemBase(WakeupManager):

    def __init__(self, system, logDefs = None):
        super(ActorSystemBase, self).__init__()
        self.system = system
        self._pendingSends = []
        if logDefs is not False: dictConfig(logDefs or defaultLoggingConfig)
        self._primaryActors = []
        self._primaryCount  = 0
        self._globalNames = {}
        self.procLimit = 0
        self._sources = {}  # key = sourcehash, value = encrypted zipfile data
        self._sourceAuthority = None  # ActorAddress of Source Authority
        self._sourceNotifications = [] # list of actor addresses to notify of loads
        asys = self._newRefAndActor(system, system.systemAddress,
                                    system.systemAddress,
                                    External)
        extreq = self._newRefAndActor(system, system.systemAddress,
                                      ActorAddress('System:ExternalRequester'),
                                      External)
        badActor = self._newRefAndActor(system, system.systemAddress,
                                        ActorAddress('System:BadActor'), BadActor)
        self.actorRegistry = {  # key=ActorAddress string, value=ActorRef
            system.systemAddress.actorAddressString: asys,
            'System:ExternalRequester': extreq,
            'System:BadActor': badActor,
        }
        self._internalAddresses = list(self.actorRegistry.keys())

        system.capabilities['Python Version'] = tuple(sys.version_info)
        system.capabilities['Thespian Generation'] = ThespianGeneration
        system.capabilities['Thespian Version'] = str(int(time.time()*1000))
        system.capabilities['Thespian ActorSystem Name'] = 'simpleSystem'
        system.capabilities['Thespian ActorSystem Version'] = 2
        system.capabilities['Thespian Watch Supported'] = False
        system.capabilities['AllowRemoteActorSources'] = 'No'

    def shutdown(self):
        while self._sources:
            self.unloadActorSource(list(self._sources.keys())[0])


    def _runSends(self, timeout=None, stop_on_available=False):
        numsends = 0
        endtime = ExpirationTimer(toTimeDeltaOrNone(timeout))
        while not endtime.expired():
            while self._pendingSends:
                numsends += 1
                if self.procLimit and numsends > self.procLimit:
                    raise RuntimeError('Too many sends')
                self._realizeWakeups()
                self._runSingleSend(self._pendingSends.pop(0))
                if stop_on_available and \
                   any([not isInternalActorSystemMessage(M)
                        for M in getattr(stop_on_available.instance,
                                         'responses', [])]):
                    return
            if endtime.remaining(forever=-1) == -1:
                return
            next_wakeup = self._next_wakeup()
            if next_wakeup is None or next_wakeup > endtime:
                return
            time.sleep(max(0, timePeriodSeconds(next_wakeup.remaining())))
            self._realizeWakeups()


    def _runSingleSend(self, ps):
        if ps.attempts > 4:
            # discard message if PoisonMessage deliveries are also
            # failing
            return
        elif ps.attempts > 2:
            if isinstance(ps.msg, PoisonMessage):
                return  # no recursion on Poison
            rcvr, sndr, msg = ps.sender, ps.toActor, \
                              PoisonMessage(ps.msg,
                                            getattr(ps, 'fail_details', None))
        else:
            rcvr, sndr, msg = ps.toActor, ps.sender, ps.msg

        tgt = self.actorRegistry.get(rcvr.actorAddressString, None)
        if not tgt:
            tgt = self.actorRegistry.get('DeadLetterBox', None)
            msg = DeadEnvelope(rcvr.actorAddressString, msg)
        if tgt:
            if rcvr == self.system.systemAddress and isinstance(msg, ValidatedSource):
                self._loadValidatedActorSource(msg.sourceHash, msg.sourceZip)
            elif tgt.instance:
                if isinstance(msg, Thespian_StatusReq):
                    self._generateStatusResponse(msg, tgt, sndr)
                else:
                    killActor = isinstance(ps.msg, ActorExitRequest)
                    self._callActorWithMessage(tgt, ps, msg, sndr)
                    if killActor and tgt not in [self.actorRegistry[key]
                                                 for key in self._internalAddresses]:
                        self._killActor(tgt, ps)
            else:
                # This is a Dead Actor and there is no
                # DeadLetterHandler.  Just discard the message
                pass
        else:
            # Target Actor no longer exists.  Handle internal
            # messages and discard all others
            pass

        if isinstance(ps.msg, ChildActorExited):
            deadAddr = ps.msg.childAddress.actorAddressString
            childref = self.actorRegistry.get(deadAddr, None)
            if childref and not childref.instance:
                # Not replaced, so complete removal, including children
                childref.shutdown()
                self.actorRegistry[deadAddr] = None


    def _realizeWakeups(self):
        "Find any expired wakeups and queue them to the send processing queue"
        for target_addr, expired in self._pop_expired_wakeups():
            self._pendingSends.append(
                PendingSend(target_addr, WakeupMessage(expired.duration), target_addr))

    def _callActorWithMessage(self, tgt, ps, msg, sndr):
        try:
            # This if is to avoid sending PoisonMessage(ChildActorExited) back to child
            if not isinstance(ps.msg, ChildActorExited) or ps.msg == msg:
                tgt.instance._receive(msg, sndr)
        except Exception as ex:
            logging.getLogger('Thespian').warning(
                'Failure of Actor %s during message processing (attempt %s)',
                tgt.address, ps.attempts,
                exc_info = True)
            ps.attempts += 1
            ps.fail_details = traceback.format_exc()
            self._pendingSends.append(ps)
        else:
            if isinstance(ps.msg, ChildActorExited):
                try:
                    del tgt._yung[tgt._yung.index(ps.msg.childAddress)]
                except ValueError:
                    pass


    def _killActor(self, tgt, ps):
        try:
            self.actorRegistry[ps.toActor.actorAddressString].instance = None
        except AttributeError:
            logging.getLogger('Thespian').warning(
                'Actor %s had no instance to reset on kill',
                ps.toActor.actorAddressString)
        for gn in self._globalNames:
            if self._globalNames[gn] == ps.toActor:
                del self._globalNames[gn]
                break
        self._sourceNotifications = list(filter(lambda N: N != ps.toActor,
                                                self._sourceNotifications))
        tgt._system._systemBase.actor_send(
            self.actorRegistry[self.system.systemAddress.actorAddressString].address,
            tgt.parent,
            ChildActorExited(ps.toActor))


    def _generateStatusResponse(self, msg, tgt, sndr):
        stsresp = Thespian_ActorStatus(tgt.address,
                                       tgt.instance.__class__.__name__,
                                       tgt._system.systemAddress)
        stsresp = self.add_wakeups_to_status(stsresp)
        for C in tgt._yung: stsresp.addChild(C)
        for M in self._pendingSends:
            if M[1] == tgt.address:
                stsresp.addPendingMessage(self.address, M[0],M[2])
        self._pendingSends.append(PendingSend(tgt.address, stsresp, sndr))

    def _newRefAndActor(self, actorSystem, parentAddr, actorAddr, actorClass,
                        sourceHash = None,
                        targetActorRequirements = None,
                        isTopLevel = False):
        try:
            actorClass = actualActorClass(actorClass,
                                          functools.partial(
                                              loadModuleFromHashSource,
                                              sourceHash, self._sources)
                                          if sourceHash else None)
            if hasattr(actorClass, 'actorSystemCapabilityCheck') and \
               not actorClass.actorSystemCapabilityCheck(
                   self.system.capabilities,
                   targetActorRequirements or {}):
                actor = None
            else:
                try:
                    actor = actorClass(childActors=None)
                except TypeError as te:
                    if "unexpected keyword argument 'childActors'" in str(te):
                        actor = actorClass()
                    else:
                        actor = None
        except ActorSystemException:
            logging.getLogger('Thespian').warning('Actor total creation failure', exc_info=True)
            actor = None
            if isTopLevel: raise
        except ImportError:
            logging.getLogger('Thespian').warning('Actor create import error for %s (hash %s)', actorClass, sourceHash, exc_info=True)
            raise
        except Exception:
            logging.getLogger('Thespian').warning('Actor total creation error', exc_info=True)
            actor = None
            if isTopLevel: raise
        nar = ActorRef(actorSystem, parentAddr, actorAddr, actor, mySourceHash=sourceHash)
        if actor:
            nar.instance._myRef = nar
            nar.instance._receive = types.MethodType(actor_base_receive, nar.instance)
        return nar

    def newPrimaryActor(self, actorClass, targetActorRequirements, globalName, sourceHash):
        "Called internally to create a new Actor instance directly under the ActorSystem."
        if globalName and globalName in self._globalNames:
            return self._globalNames[globalName]
        logger = logging.getLogger('Thespian')
        naa = _newAddress("/A", self._primaryCount)
        self._primaryCount = self._primaryCount + 1
        nar = self._newRefAndActor(self.system, self.system.systemAddress, naa,
                                   actorClass, sourceHash,
                                   targetActorRequirements = targetActorRequirements,
                                   isTopLevel = True)
        if nar.instance:
            if globalName:
                self._globalNames[globalName] = naa
                logger.info('Registered %s as global "%s" Primary Actor',
                            str(naa), globalName)
        if not nar.instance:
            logger.warning('Could not create primary Actor %s @ %s',
                           str(actorClass), str(naa))
            return self.actorRegistry['System:BadActor'].address
        self.actorRegistry[naa.actorAddressString] = nar
        logger.debug('Created primary Actor %s @ %s', str(actorClass), str(naa))
        return naa

    def newActor(self, parentAddr, actorClass, actorSystem, targetActorRequirements, globalName, sourceHash):
        if globalName and globalName in self._globalNames:
            return self._globalNames[globalName]
        pa = self.actorRegistry.get(parentAddr.actorAddressString, None)
        if not pa:
            raise InvalidActorAddress(parentAddr,
                                      'invalid parent Address for new "%s" actor'%str(actorClass))
        naa = _newChildAddress(pa)
        # register *before* creating the child to lock-in the address
        # and because child init failure will result in a
        # ChildActorExited message to this parent.
        pa.addChild(naa)
        # Now create child and add it to the ActorSystem's registry
        self.actorRegistry[naa.actorAddressString] = nar = \
                self._newRefAndActor(actorSystem, parentAddr, naa, actorClass,
                                     targetActorRequirements = targetActorRequirements,
                                     sourceHash = sourceHash)
        if nar and nar.instance:
            if globalName:
                logging.getLogger('Thespian').info('Registered %s as global "%s" Actor',
                                                   str(naa), globalName)
                self._globalNames[globalName] = naa
        else:
            self.actor_send(self.system.systemAddress, parentAddr, ChildActorExited(naa))
        logging.getLogger('Thespian').debug('Created Actor %s @ %s as child of %s'%(
                                             str(actorClass), str(naa), str(parentAddr)))
        return naa


    def tell(self, anActor, msg):
        self._realizeWakeups()   # First, so that they "fire" between the last call and this one
        self._pendingSends.append(PendingSend(self.actorRegistry['System:ExternalRequester'].address, msg, anActor))
        self._runSends()

    def ask(self, anActor, msg, timeout):
        self._realizeWakeups()   # First, so that they "fire" between the last call and this one
        sender = self.actorRegistry['System:ExternalRequester']
        self._pendingSends.append(PendingSend(sender.address, msg, anActor))
        return self.listen(timeout)

    def listen(self, timeout):
        # timeout is ignored because we are executing in the context
        # of the current thread, so all actors will run to completion
        # synchronously (or block on external operations) and timeout
        # cannot be effectively implemented.  At best, runSends could
        # check remaining time between Actor calls and return if the
        # timeout period has been exceeded, but that still wouldn't
        # allow interruption of blocked Actors.
        sender = self.actorRegistry['System:ExternalRequester']
        self._runSends(timeout, stop_on_available=sender)
        while getattr(sender.instance, 'responses', None):
            response = sender.instance.responses.pop(0)
            if isInternalActorSystemMessage(response): continue
            return response
        return None

    def actor_send(self, fromActor, toActor, msg):
        self._pendingSends.append(PendingSend(fromActor, msg, toActor))

    def wakeupAfter(self, fromActor, timePeriod):
        self._add_wakeup(fromActor, timePeriod)
        self._realizeWakeups()

    def _handleDeadLetters(self, address, enable):
        self._realizeWakeups()
        reg = self.actorRegistry.get(address.actorAddressString, None)
        if enable:
            self.actorRegistry['DeadLetterBox'] = reg
        else:
            if reg == self.actorRegistry.get('DeadLetterBox', None):
                self.actorRegistry['DeadLetterBox'] = None

    def setProcessingLimit(self, limit=0):
        self.procLimit = limit


    def updateCapability(self, capabilityName, capabilityValue):
        if capabilityValue is None:
            if capabilityName in self.system.capabilities:
                del self.system.capabilities[capabilityName]
        else:
            self.system.capabilities[capabilityName] = capabilityValue


    def registerSourceAuthority(self, address):
        self._sourceAuthority = address


    def registerSourceNotifications(self, address, enable):
        all_except = list(filter(lambda a: a != address, self._sourceNotifications))
        if enable:
            self._sourceNotifications = all_except + [address]
            for each_hash in self._sources:
                self.actor_send(self.system.systemAddress,
                                address,
                                LoadedSource(each_hash, ''))  # no info available
        else:
            self._sourceNotifications = all_except

    def loadActorSource(self, fname):
        import hashlib
        f = fname if hasattr(fname, 'read') else open(fname, 'rb')
        try:
            d = f.read()
        finally:
            f.close()
        hval = hashlib.md5(d).hexdigest()
        logging.getLogger('Thespian').info('Loaded source %s hash %s', fname, hval)

        if self._sourceAuthority:
            self._pendingSends.append(PendingSend(self.system.systemAddress,
                                                  ValidateSource(hval, d),
                                                  self._sourceAuthority))
            self._runSends()
        return hval

    def _loadValidatedActorSource(self, sourceHash, sourceZip):
        # Validate the source file by constructing a SourceHashFinder
        # for it and seeing if that SourceHashFinder can access the
        # contents.
        try:
            f = SourceHashFinder(sourceHash, lambda v: v, sourceZip)
            namelist = f.getZipNames()
            logging.getLogger('Thespian').info(
                'Validated source hash %s, %s modules (%s)',
                sourceHash, len(namelist),
                ', '.join(namelist if len(namelist) < 10 else
                          namelist[:9] + ['...']))
        except Exception as ex:
            logging.getLogger('Thespian').error('Validated source (hash %s) is corrupted: %s',
                                                sourceHash, ex)
            return

        # Store this registered source
        self._sources[sourceHash] = sourceZip

        # Generate notifications
        for each_target in self._sourceNotifications:
            self.actor_send(self.system.systemAddress, each_target,
                            LoadedSource(sourceHash, ''))  # no info available


    def unloadActorSource(self, sourceHash):
        if sourceHash in self._sources:
            del self._sources[sourceHash]

            # Generate notifications
            for each_target in self._sourceNotifications:
                self.actor_send(self.system.systemAddress, each_target,
                                UnloadedSource(sourceHash, ''))  # no info available

        for pnum, metapath in enumerate(sys.meta_path):
            if getattr(metapath, 'srcHash', None) == sourceHash:
                rmmods = [M for M in sys.modules
                          if M and M.startswith(metapath.hashRoot())]
                for each in rmmods:
                    del sys.modules[each]
                del sys.meta_path[pnum]
                break
