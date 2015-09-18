'''The systemBase provides the base class implementation for standard
system Base implementations.  This systemBase itself is not intended
to be instantiated as the regular Thespian System Base, but instead it
provides a base class that should be subclassed by the various System
Base implementations.

'''

import logging, logging.handlers
try:
    from logging.config import dictConfig
except ImportError:
    # Old python that doesn't contain this...
    from thespian.system.dictconfig import dictConfig

from thespian.actors import *
from thespian.system import *
from thespian.system.utilis import toTimeDeltaOrNone, thesplog, ExpiryTime
from thespian.system.messages.admin import *
from thespian.system.messages.status import *
from thespian.system.transport import *

from datetime import datetime, timedelta
import os

MAX_SYSTEM_SHUTDOWN_DELAY    = timedelta(seconds=10)
MAX_CHILD_ACTOR_CREATE_DELAY = timedelta(seconds=5)
MAX_CAPABILITY_UPDATE_DELAY  = timedelta(seconds=5)
MAX_LOAD_SOURCE_DELAY        = timedelta(seconds=61)
MAX_ADMIN_STATUS_REQ_DELAY   = timedelta(seconds=2)
MAX_TELL_PERIOD              = timedelta(seconds=60)


class systemBase(object):

    """This is the systemBase base class that various Thespian System Base
       implementations should subclass.  The System Base is
       instantiated by each process that wishes to utilize an Actor
       System and runs in the context of that process (as opposed to
       the System Admin that may run in its own process).

       Depending on the System Base implementation chosen by that
       process, the instantiation may be private to that process or
       shared by other processes; in the former case, there will be an
       instance of this class in each process accessing the shared
       Actor System, representing the Portal between the "external"
       environment of that process and the shared Actor System
       Implementation.

       All ActorAddresses generated via newActor and newPrimaryActor
       are local to this ActorSystemBase instance.  Any and *all*
       messages sent to other Actors must be able to be appropriately
       serialized; this allows the pickling/unpickling process to
       translate an ActorAddress from a local representation to a
       global or remote representation.

    """

    def __init__(self, system, logDefs = None):
        self._numPrimaries = 0
        # Expects self.transport has already been set by subclass __init__
        self.adminAddr = self.transport.getAdminAddr(system.capabilities)
        tryingTime = ExpiryTime(MAX_SYSTEM_SHUTDOWN_DELAY + timedelta(seconds=1))
        while not tryingTime.expired():
            if not self.transport.probeAdmin(self.adminAddr):
                self._startAdmin(self.adminAddr,
                                 self.transport.myAddress,
                                 system.capabilities,
                                 logDefs)
            if self._verifyAdminRunning(): return
            import time
            time.sleep(0.5)  # Previous version may have been exiting

        if not self._verifyAdminRunning():
            raise InvalidActorAddress(self.adminAddr,
                                          'not a valid or useable ActorSystem Admin')
            # KWQ: more details? couldn't start @ addr? response was ? instead of expected Thespian_SystemStatus?


    def _verifyAdminRunning(self):
        """Returns boolean verification that the Admin is running and
           available.  Will query the admin for a positive response,
           blocking until one is received.
        """
        self._VERIFYFAILED = False
        self.transport.scheduleTransmit(
            None,
            TransmitIntent(self.adminAddr, QueryExists(),
                           onError=self._verifySendFailed))
        response = self.transport.run(None, MAX_ADMIN_STATUS_REQ_DELAY)
        return not getattr(self, '_VERIFYFAILED', False) and \
            response and \
            response.sender == self.adminAddr and \
            isinstance(response.message, QueryAck) \
            and not response.message.inShutdown


    def _verifySendFailed(self, result, msg):
        self._VERIFYFAILED = True
        self.transport.abort_run()



    def __getstate__(self):
        raise CannotPickle('ActorSystem cannot be Pickled.')

    def shutdown(self):
        thesplog('ActorSystem shutdown requested.', level=logging.INFO)
        time_to_quit = ExpiryTime(MAX_SYSTEM_SHUTDOWN_DELAY)
        self.transport.scheduleTransmit(
            None,
            TransmitIntent(self.adminAddr, SystemShutdown(),
                           onError=self._shutdownSendFailed))
        while not time_to_quit.expired():
            response = self.transport.run(None, time_to_quit.remaining())
            if getattr(self, '_TASF', False):
                thesplog('Could not send shutdown request to Admin'
                         '; aborting but not necessarily stopped',
                         level=logging.WARNING)
                return
            if response:
                if isinstance(response.message, SystemShutdownCompleted):
                    break
                else:
                    thesplog('Expected shutdown completed message, got: %s', response.message,
                             level=logging.WARNING)
            else:
                thesplog('No response to Admin shutdown request; Actor system not completely shutdown',
                         level=logging.ERROR)
        thesplog('ActorSystem shutdown complete.')

    def _shutdownSendFailed(self, result, msg):
        self._TASF = True
        thesplog('ActorSystem shutdown request failed.', level=logging.WARNING)
        self.transport.abort_run()


    def newPrimaryActor(self, actorClass, targetActorRequirements, globalName, sourceHash=None):
        self._numPrimaries = self._numPrimaries + 1
        self._pcrFAILED = False
        self._newActorAddress = None
        actorClassName = '%s.%s'%(actorClass.__module__,
                                  actorClass.__name__) if hasattr(actorClass, '__name__') else actorClass
        self.transport.scheduleTransmit(
            None,
            TransmitIntent(self.adminAddr,
                           PendingActor(actorClassName,
                                        None, self._numPrimaries,
                                        targetActorRequirements,
                                        globalName=globalName,
                                        sourceHash=sourceHash),
                           onError = self._newPrimarySendFailed))
        response = self.transport.run(self._newActorCallback,
                                      MAX_CHILD_ACTOR_CREATE_DELAY)
        if self._pcrFAILED:
            if self._pcrFAILED == PendingActorResponse.ERROR_Invalid_SourceHash:
                raise InvalidActorSourceHash(sourceHash)
            if self._pcrFAILED == PendingActorResponse.ERROR_Invalid_ActorClass:
                raise InvalidActorSpecification(actorClass)
            if self._pcrFAILED == PendingActorResponse.ERROR_Import:
                raise ImportError(actorClass)
            if self._pcrFAILED == PendingActorResponse.ERROR_No_Compatible_ActorSystem:
                raise NoCompatibleSystemForActor(
                    actorClass, 'No compatible ActorSystem could be found')
            raise ActorSystemFailure("Could not request new Actor from Admin")
        if self._newActorAddress:
            return self._newActorAddress
        if self._newActorAddress is False:
            raise NoCompatibleSystemForActor(
                actorClass, 'No compatible ActorSystem could be found')
        raise ActorSystemRequestTimeout('No response received to PendingActor request to Admin')

    def _newPrimarySendFailed(self, result, msg):
        self._pcrFAILED = True
        self.transport.abort_run()

    def _newActorCallback(self, envelope):
        if isinstance(envelope.message, PendingActorResponse):
            self._newActorAddress = False if envelope.message.errorCode else \
                                    envelope.message.actualAddress
            self._pcrFAILED = envelope.message.errorCode
            return False # Stop running transport; got new actor address (or failure)
        # Discard everything else.  Previous requests and operations
        # may have caused there to be messages sent back to this
        # endpoint that are queued ahead of the PendingActorResponse.
        return True  # Keep waiting for the PendingActorResponse


    def tell(self, anActor, msg):
        attemptLimit = ExpiryTime(MAX_TELL_PERIOD)
        import socket
        for attempt in range(5000):
            try:
                self.transport.scheduleTransmit(
                    None,
                    TransmitIntent(anActor, msg, onError=self._tellFailed))
                while not attemptLimit.expired():
                    if not self.transport.run(TransmitOnly, attemptLimit.remaining()):
                        break  # all transmits completed
                return
            except socket.error as ex:
                import errno
                if errno.EMFILE == ex.errno:
                    import time
                    time.sleep(0.1)
                else:
                    raise

    def _tellFailed(self, result, intent):
        if result == SendStatus.DeadTarget:
            self.transport.scheduleTransmit(
                None,
                TransmitIntent(self.adminAddr,
                               DeadEnvelope(intent.targetAddr,
                                            intent.message))) # error ignored
        else:
            raise ActorSystemFailure('tell to %s failed with: %s'%(
                str(intent.targetAddr), str(result)))


    def listen(self, timeout):
        while True:
            response = self.transport.run(None, toTimeDeltaOrNone(timeout))
            if response is None: break
            # Do not send miscellaneous ActorSystemMessages to the caller
            # that it might not recognize.
            if response and not isInternalActorSystemMessage(response.message):
                return response.message
        return None

    def ask(self, anActor, msg, timeout):
        self._ASKFAILED = None
        self.transport.scheduleTransmit(
            None,
            TransmitIntent(anActor, msg, onError = self._askSendFailed))

        while True:
            response = self.transport.run(None, toTimeDeltaOrNone(timeout))
            if response is None: break
            if self._ASKFAILED is not None:
                if self._ASKFAILED in [SendStatus.DeadTarget,
                                       SendStatus.Failed,
                                       SendStatus.NotSent]:
                    # Silent failure; not all transports can indicate
                    # this, so for conformity the Dead Letter handler is
                    # the intended method of handling this issue.
                    return None
                raise ActorSystemFailure('Transmit of ask message to %s failed (%s)'%(
                    str(anActor),
                    str(self._ASKFAILED)))
            # Do not send miscellaneous ActorSystemMessages to the caller
            # that it might not recognize.
            if response and not isInternalActorSystemMessage(response.message):
                return response.message
        return None

    def _askSendFailed(self, result, intent):
        if result == SendStatus.DeadTarget:
            self.transport.scheduleTransmit(
                None,
                TransmitIntent(self.adminAddr,
                               DeadEnvelope(intent.targetAddr, intent.message))) # error ignored
        self._ASKFAILED = result
        # Reached here on a callback from run(), so cause an exit from this run().
        self.transport.abort_run()


    def updateCapability(self, capabilityName, capabilityValue=None):
        self._updCAPFAILED = False
        attemptLimit = ExpiryTime(MAX_CAPABILITY_UPDATE_DELAY)
        self.transport.scheduleTransmit(
            None,
            TransmitIntent(self.adminAddr,
                           CapabilityUpdate(capabilityName, capabilityValue),
                           onError = self._updateCapsFailed))
        while not attemptLimit.expired():
            if not self.transport.run(TransmitOnly, attemptLimit.remaining()):
                break  # all transmits completed
        if self._updCAPFAILED or attemptLimit.expired():
            raise ActorSystemFailure("Could not update Actor System Admin capabilities.")


    def _updateCapsFailed(self, result, msg):
        self._updCAPFAILED = True
        self.transport.abort_run()


    def loadActorSource(self, fname):
        self._LOADFAILED = None
        loadLimit = ExpiryTime(MAX_LOAD_SOURCE_DELAY)
        f = fname if hasattr(fname, 'read') else open(fname, 'rb')
        try:
            d = f.read()
            import hashlib
            hval = hashlib.md5(d).hexdigest()
            self.transport.scheduleTransmit(None,
                                            TransmitIntent(self.adminAddr,
                                                           ValidateSource(hval, d),
                                                           onError = self._loadReqFailed))
            while not loadLimit.expired():
                if not self.transport.run(TransmitOnly, loadLimit.remaining()):
                    break  # all transmits completed
            if self._LOADFAILED or loadLimit.expired():
                raise ActorSystemFailure('Load source failed due to ' +
                                         ('failure response (%s)'%self._LOADFAILED
                                          if self._LOADFAILED else
                                          'timeout (%s)'%str(loadLimit)))
            return hval
        finally:
            f.close()


    def _loadReqFailed(self, result, msg):
        self._LOADFAILED = result
        self.transport.abort_run()


    def unloadActorSource(self, sourceHash):
        self._LOADFAILED = None
        loadLimit = ExpiryTime(MAX_LOAD_SOURCE_DELAY)
        self.transport.scheduleTransmit(None,
                                        TransmitIntent(self.adminAddr,
                                                       ValidateSource(sourceHash, None),
                                                       onError = self._loadReqFailed))
        while not loadLimit.expired():
            if not self.transport.run(TransmitOnly, loadLimit.remaining()):
                break  # all transmits completed
        if self._LOADFAILED or loadLimit.expired():
            raise ActorSystemFailure('Unload source failed due to ' +
                                     ('failure response' if self._LOADFAILED else
                                      'timeout (%s)'%str(loadLimit)))
