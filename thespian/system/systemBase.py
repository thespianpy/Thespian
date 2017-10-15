'''The systemBase provides the base class implementation for standard
system Base implementations.  This systemBase itself is not intended
to be instantiated as the regular Thespian System Base, but instead it
provides a base class that should be subclassed by the various System
Base implementations.

'''

import logging
from thespian.actors import *
from thespian.system import *
from thespian.system.utilis import thesplog
from thespian.system.timing import toTimeDeltaOrNone, ExpirationTimer, unexpired
from thespian.system.messages.admin import *
from thespian.system.messages.status import *
from thespian.system.transport import *
import threading
from contextlib import closing
from datetime import timedelta
import os


MAX_SYSTEM_SHUTDOWN_DELAY    = timedelta(seconds=10)
MAX_CHILD_ACTOR_CREATE_DELAY = timedelta(seconds=50)
MAX_CAPABILITY_UPDATE_DELAY  = timedelta(seconds=5)
MAX_LOAD_SOURCE_DELAY        = timedelta(seconds=61)
MAX_ADMIN_STATUS_REQ_DELAY   = timedelta(seconds=2)
MAX_TELL_PERIOD              = timedelta(seconds=60)

def ensure_TZ_set():
    # Actor engines handle timeouts and tend to sample system time
    # frequently.  Under Linux, if TZ is not set to a value,
    # /etc/localtime or similar is consulted on each call to obtain
    # system time which can negatively affect performance.  This
    # function attempts to set TZ if possible/reasonable.
    if 'TZ' in os.environ:
        return
    for fname in ('/etc/localtime',
                  '/usr/local/etc/localtime'):
        if os.path.exists(fname):
            os.environ['TZ'] = ':' + fname
            return
    # OK if it's not set, just may be slower


class TransmitTrack(object):

    def __init__(self, transport, adminAddr):
        self._newActorAddress = None
        self._pcrFAILED = None
        self._transport = transport
        self._adminAddr = adminAddr

    @property
    def failed(self):
        return self._pcrFAILED is not None

    @property
    def failure(self):
        return self._pcrFAILED

    @property
    def failure_message(self):
        return getattr(self, '_pcrMessage', None)

    def transmit_failed(self, result, intent):
        if result == SendStatus.DeadTarget and \
           intent.targetAddr != self._adminAddr:
            # Forward message to the dead letter handler; if the
            # forwarding fails, just discard the message.
            self._transport.scheduleTransmit(
                None,
                TransmitIntent(self._adminAddr,
                                DeadEnvelope(intent.targetAddr, intent.message)))
        self._pcrFAILED = result
        self._transport.abort_run()


class NewActorResponse(TransmitTrack):

    def __init__(self, transport, adminAddr, *args, **kw):
        super(NewActorResponse, self).__init__(transport, adminAddr, *args, **kw)
        self._newActorAddress = None

    @property
    def pending(self):
        return self._newActorAddress is None and not self.failed

    @property
    def actor_address(self):
        return self._newActorAddress

    def __call__(self, envelope):
        if isinstance(envelope.message, PendingActorResponse):
            self._newActorAddress = False if envelope.message.errorCode else \
                                    envelope.message.actualAddress
            self._pcrFAILED = envelope.message.errorCode
            self._pcrMessage = getattr(envelope.message, 'errorStr', None)
            # Stop running transport; got new actor address (or failure)
            return False
        # Discard everything else.  Previous requests and operations
        # may have caused there to be messages sent back to this
        # endpoint that are queued ahead of the PendingActorResponse.
        return True  # Keep waiting for the PendingActorResponse



class ExternalOpsToActors(object):

    def __init__(self, adminAddr, transport=None):
        self._numPrimaries = 0
        self._cv = threading.Condition()
        self._transport_runner = False
        # Expects self.transport has already been set by subclass __init__
        self.adminAddr = adminAddr
        if transport:
            self.transport = transport


    def _run_transport(self, maximumDuration=None, txonly=False,
                       incomingHandler=None):
        # This is where multiple external threads are synchronized for
        # receives.  Transmits will flow down into the transmit layer
        # where they are queued with thread safety, but threads
        # blocking on a receive will all be lined up through this point.

        max_runtime = ExpirationTimer(maximumDuration)

        with self._cv:
            while self._transport_runner:
                self._cv.wait(max_runtime.view().remainingSeconds())
                if max_runtime.view().expired():
                    return None
            self._transport_runner = True

        try:
            r = Thespian__UpdateWork()
            while isinstance(r, Thespian__UpdateWork):
                r = self.transport.run(TransmitOnly if txonly else incomingHandler,
                                       max_runtime.view().remaining())
            return r
            # incomingHandler callback could deadlock on this same thread; is it ever not None?
        finally:
            with self._cv:
                self._transport_runner = False
                self._cv.notify()


    def _tx_to_actor(self, actorAddress, message):
        # Send a message from this external process to an actor.
        # Returns a TransmitTrack object that can be used to check for
        # transmit errors.
        txwatch = TransmitTrack(self.transport, self.adminAddr)
        self.transport.scheduleTransmit(
            None,
            TransmitIntent(actorAddress, message,
                           onError=txwatch.transmit_failed))
        return txwatch


    def _tx_to_admin(self, message):
        return self._tx_to_actor(self.adminAddr, message)


    def newPrimaryActor(self, actorClass, targetActorRequirements, globalName,
                        sourceHash=None):
        self._numPrimaries = self._numPrimaries + 1
        actorClassName = '%s.%s'%(actorClass.__module__, actorClass.__name__) \
                         if hasattr(actorClass, '__name__') else actorClass
        with closing(self.transport.external_transport_clone()) as tx_external:
            response = NewActorResponse(tx_external, self.adminAddr)
            tx_external.scheduleTransmit(
                None,
                TransmitIntent(self.adminAddr,
                               PendingActor(actorClassName,
                                            None, self._numPrimaries,
                                            targetActorRequirements,
                                            globalName=globalName,
                                            sourceHash=sourceHash),
                               onError=response.transmit_failed))
            endwait = ExpirationTimer(MAX_CHILD_ACTOR_CREATE_DELAY)
            # Do not use _run_transport: the tx_external transport
            # context acquired above is unique to this thread and
            # should not be synchronized/restricted by other threads.
            tx_external.run(response, MAX_CHILD_ACTOR_CREATE_DELAY)
            # Other items might abort the transport run... like transmit
            # failures on a previous ask() that itself already timed out.
            while response.pending and not endwait.view().expired():
                tx_external.run(response, MAX_CHILD_ACTOR_CREATE_DELAY)

        if response.failed:
            if response.failure == PendingActorResponse.ERROR_Invalid_SourceHash:
                raise InvalidActorSourceHash(sourceHash)
            if response.failure == PendingActorResponse.ERROR_Invalid_ActorClass:
                raise InvalidActorSpecification(actorClass,
                                                response.failure_message)
            if response.failure == PendingActorResponse.ERROR_Import:
                info = response.failure_message
                if info:
                    thesplog('Actor Create Failure, Import Error: %s', info)
                    raise ImportError(str(actorClass) + ': ' + info)
                thesplog('Actor Create Failure, Import Error')
                raise ImportError(actorClass)
            if response.failure == PendingActorResponse.ERROR_No_Compatible_ActorSystem:
                raise NoCompatibleSystemForActor(
                    actorClass, 'No compatible ActorSystem could be found')
            raise ActorSystemFailure("Could not request new Actor from Admin (%s)"
                                     % (response.failure))
        if response.actor_address:
            return response.actor_address
        if response.actor_address is False:
            raise NoCompatibleSystemForActor(
                actorClass, 'No compatible ActorSystem could be found')
        raise ActorSystemRequestTimeout(
            'No response received to PendingActor request to Admin'
            ' at %s from %s'%(str(self.adminAddr),
                              str(self.transport.myAddress)))


    def tell(self, anActor, msg):
        attemptLimit = ExpirationTimer(MAX_TELL_PERIOD)
        # transport may not use sockets, but this helps error handling
        # in case it does.
        import socket
        for attempt in range(5000):
            try:
                txwatch = self._tx_to_actor(anActor, msg)
                for attemptTime in unexpired(attemptLimit):
                    if not self._run_transport(attemptTime.remaining(),
                                               txonly=True):
                        # all transmits completed
                        return
                    if txwatch.failed:
                        raise ActorSystemFailure(
                            'Error sending to %s: %s' % (str(anActor),
                                                         str(txwatch.failure)))
                raise ActorSystemRequestTimeout(
                    'Unable to send to %s within %s' %
                    (str(anActor), str(MAX_TELL_PERIOD)))
            except socket.error as ex:
                import errno
                if errno.EMFILE == ex.errno:
                    import time
                    time.sleep(0.1)
                else:
                    raise


    def listen(self, timeout):
        while True:
            response = self._run_transport(toTimeDeltaOrNone(timeout))
            if not isinstance(response, ReceiveEnvelope):
                break
            # Do not send miscellaneous ActorSystemMessages to the caller
            # that it might not recognize.
            if not isInternalActorSystemMessage(response.message):
                return response.message
        return None

    def ask(self, anActor, msg, timeout):
        txwatch = self._tx_to_actor(anActor, msg)  # KWQ: pass timeout on tx??
        askLimit = ExpirationTimer(toTimeDeltaOrNone(timeout))
        for remTime in unexpired(askLimit):
            response = self._run_transport(remTime.remaining())
            if txwatch.failed:
                if txwatch.failure in [SendStatus.DeadTarget,
                                       SendStatus.Failed,
                                       SendStatus.NotSent]:
                    # Silent failure; not all transports can indicate
                    # this, so for conformity the Dead Letter handler is
                    # the intended method of handling this issue.
                    return None
                raise ActorSystemFailure('Transmit of ask message to %s failed (%s)'%(
                    str(anActor),
                    str(txwatch.failure)))
            if not isinstance(response, ReceiveEnvelope):
                # Timed out or other failure, give up.
                break
            # Do not send miscellaneous ActorSystemMessages to the
            # caller that it might not recognize.  If one of those was
            # recieved, loop to get another response.
            if not isInternalActorSystemMessage(response.message):
                return response.message
        return None



class systemBase(ExternalOpsToActors):

    """This is the systemBase base class that various Thespian System Base
       implementations should subclass.  The System Base is
       instantiated by each process that wishes to utilize an Actor
       System and runs in the context of that process (as opposed to
       the System Admin that may run in its own process).

       This base is not present in the Actors themselves, only in the
       external application that wish to talk to Actors.

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
        ensure_TZ_set()

        # Expects self.transport has already been set by subclass __init__
        super(systemBase, self).__init__(
            self.transport.getAdminAddr(system.capabilities))

        tryingTime = ExpirationTimer(MAX_SYSTEM_SHUTDOWN_DELAY + timedelta(seconds=1))
        while not tryingTime.view().expired():
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
        txwatch = self._tx_to_admin(QueryExists())
        response = self._run_transport(MAX_ADMIN_STATUS_REQ_DELAY)
        return not txwatch.failed and \
            isinstance(response, ReceiveEnvelope) and \
            isinstance(response.message, QueryAck) \
            and not response.message.inShutdown


    def __getstate__(self):
        raise CannotPickle('ActorSystem cannot be Pickled.')

    def shutdown(self):
        thesplog('ActorSystem shutdown requested.', level=logging.INFO)
        time_to_quit = ExpirationTimer(MAX_SYSTEM_SHUTDOWN_DELAY)
        txwatch = self._tx_to_admin(SystemShutdown())
        for remaining_time in unexpired(time_to_quit):
            response = self._run_transport(remaining_time.remaining())
            if txwatch.failed:
                thesplog('Could not send shutdown request to Admin'
                         '; aborting but not necessarily stopped',
                         level=logging.WARNING)
                return
            if isinstance(response, ReceiveEnvelope):
                if isinstance(response.message, SystemShutdownCompleted):
                    break
                else:
                    thesplog('Expected shutdown completed message, got: %s', response.message,
                             level=logging.WARNING)
            elif isinstance(response, (Thespian__Run_Expired,
                                       Thespian__Run_Terminated,
                                       Thespian__Run_Expired)):
                break
            else:
                thesplog('No response to Admin shutdown request; Actor system not completely shutdown',
                         level=logging.ERROR)
        self.transport.close()
        thesplog('ActorSystem shutdown complete.')


    def updateCapability(self, capabilityName, capabilityValue=None):
        attemptLimit = ExpirationTimer(MAX_CAPABILITY_UPDATE_DELAY)
        txwatch = self._tx_to_admin(CapabilityUpdate(capabilityName,
                                                     capabilityValue))
        for remaining_time in unexpired(attemptLimit):
            if not self._run_transport(remaining_time.remaining(), txonly=True):
                return  # all transmits completed
            if txwatch.failed:
                raise ActorSystemFailure(
                    'Error sending capability updates to Admin: %s' %
                    str(txwatch.failure))
        raise ActorSystemRequestTimeout(
            'Unable to confirm capability update in %s' %
            str(MAX_CAPABILITY_UPDATE_DELAY))


    def loadActorSource(self, fname):
        loadLimit = ExpirationTimer(MAX_LOAD_SOURCE_DELAY)
        f = fname if hasattr(fname, 'read') else open(fname, 'rb')
        try:
            d = f.read()
            import hashlib
            hval = hashlib.md5(d).hexdigest()
            txwatch = self._tx_to_admin(
                ValidateSource(hval, d, getattr(f, 'name',
                                                str(fname)
                                                if hasattr(fname, 'read')
                                                else fname)))
            for load_time in unexpired(loadLimit):
                if not self._run_transport(load_time.remaining(), txonly=True):
                    # All transmits completed
                    return hval
                if txwatch.failed:
                    raise ActorSystemFailure(
                        'Error sending source load to Admin: %s' %
                        str(txwatch.failure))
            raise ActorSystemRequestTimeout('Load source timeout: ' +
                                            str(loadLimit))
        finally:
            f.close()


    def unloadActorSource(self, sourceHash):
        loadLimit = ExpirationTimer(MAX_LOAD_SOURCE_DELAY)
        txwatch = self._tx_to_admin(ValidateSource(sourceHash, None))
        for load_time in unexpired(loadLimit):
            if not self._run_transport(load_time.remaining(), txonly=True):
                return  # all transmits completed
            if txwatch.failed:
                raise ActorSystemFailure(
                    'Error sending source unload to Admin: %s' %
                    str(txwatch.failure))
        raise ActorSystemRequestTimeout('Unload source timeout: ' +
                                        str(loadLimit))


    def external_clone(self):
        """Get a separate local endpoint that does not commingle traffic with
           the the main ActorSystem or other contexts.  Makes internal
           blocking calls, so primarily appropriate for a
           multi-threaded client environment.
        """
        return BaseContext(self.adminAddr, self.transport)


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Actors that involve themselves in topology

    def preRegisterRemoteSystem(self, remoteAddress, remoteCapabilities):
        self.send(self.adminAddr,
                  ConventionRegister(
                      self.transport.getAddressFromString(remoteAddress),
                      remoteCapabilities,
                      preRegister=True))

    def deRegisterRemoteSystem(self, remoteAddress):
        self.send(
            self.adminAddr,
            ConventionDeRegister(
                remoteAddress
                if isinstance(remoteAddress, ActorAddress) else
                self.transport.getAddressFromString(remoteAddress)))


class BaseContext(ExternalOpsToActors):
    def __init__(self, adminAddr, transport):
        super(BaseContext, self).__init__(adminAddr,
                                          transport.external_transport_clone())
    def exit_context(self):
        self.transport.close()
