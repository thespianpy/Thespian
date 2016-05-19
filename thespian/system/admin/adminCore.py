import sys
import logging
from thespian.actors import *
from thespian.system.utilis import thesplog
from thespian.system.systemCommon import systemCommonBase
from thespian.system.messages.status import Thespian_SystemStatus
from thespian.system.messages.admin import *
from thespian.system.messages.logcontrol import SetLogging
from thespian.system.transport import TransmitIntent
from thespian.system.sourceLoader import SourceHashFinder


class AdminCore(systemCommonBase):

    def __init__(self, transport, address, capabilities, logdefs):
        thesplog('++++ Starting Admin from %s', sys.modules['thespian'].__file__, level=logging.DEBUG)
        super(AdminCore, self).__init__(address, transport)
        self.capabilities = capabilities
        self.logdefs      = logdefs
        self._pendingChildren = {}  # key = childLocalAddr instance #, value = PendingActorEnvelope
        # Things that help us look like an Actor, even though we're not
        self._sourceHash  = None
        thesplog('++++ Admin started @ %s / gen %s', self.transport.myAddress, str(ThespianGeneration), level=logging.INFO, primary=True)
        self._nannying = {}  # key=child actorAddress, value=parent Address
        self._deadLetterHandler = None
        self._sources = {}  # Index is sourcehash, value is requestor
                            # ActorAddress or zipsrc (when validated)
        self._sourceAuthority = None


    def _activate(self):
        """Called when the full ActorSystem initialization is completed.  This
           should then activate any functionality that needed to wait
           for completion of initialization.
        """
        pass


    def run(self):
        try:
            self.transport.run(self.handleIncoming, None)
        except Exception as ex:
            import traceback
            thesplog('ActorAdmin uncaught exception: %s', traceback.format_exc(),
                     level=logging.ERROR, exc_info=True)
        thesplog('Admin time to die', level=logging.DEBUG)


    def handleIncoming(self, envelope):
        self._sCBStats.inc('Admin Message Received.Total')
        handled, result = self._handleReplicatorMessages(envelope)
        if handled:
            return result
        if isinstance(envelope.message, (ActorSystemMessage, logging.LogRecord)):
            thesplog('Admin of %s', envelope.identify(), level=logging.DEBUG)
            return getattr(self,
                           'h_' + envelope.message.__class__.__name__, self.unrecognized)(envelope)
        # else discard random non-admin messages
        self._sCBStats.inc('Admin Message Received.Ignored')
        thesplog('ADMIN DISCARD %s', envelope.identify(), level=logging.WARNING)
        return True


    def unrecognized(self, envelope):
        self._sCBStats.inc('Admin Message Received.Discarded')
        thesplog("Admin got incoming %s from %s; discarded because I don't know how to handle it!",
                 envelope.message, envelope.sender, level=logging.WARNING, primary=True)
        return True


    def isShuttingDown(self): return hasattr(self, '_exiting')


    def h_QueryExists(self, envelope):
        self._sCBStats.inc('Admin Message Received.Type.QueryExists')
        self._send_intent(
            TransmitIntent(envelope.sender,
                           QueryAck(
                               self.capabilities.get('Thespian ActorSystem Name',
                                                     'misc Actor System'),
                               self.capabilities.get('Thespian ActorSystem Version',
                                                     'unknown Version'),
                               self.isShuttingDown())))
        return True


    def getStatus(self):
        resp = Thespian_SystemStatus(self.myAddress,
                                     capabilities = self.capabilities,
                                     inShutdown = self.isShuttingDown())
        resp.setDeadLetterHandler(self._deadLetterHandler)
        self._updateStatusResponse(resp)
        resp.setLoadedSources(list(self._sources.keys()))
        resp.sourceAuthority = self._sourceAuthority
        return resp

    def h_Thespian_StatusReq(self, envelope):
        self._sCBStats.inc('Admin Message Received.Type.StatusReq')
        self._send_intent(TransmitIntent(envelope.sender, self.getStatus()))
        return True


    def thesplogStatus(self):
        "Write status to thesplog"
        from io import StringIO
        sd = StringIO()
        from thespian.system.messages.status import formatStatus
        try:
            sd.write('')
            SSD = lambda v: v
        except TypeError:
            class SSD(object):
                def __init__(self, sd): self.sd = sd
                def write(self, str_arg): self.sd.write(str_arg.decode('utf-8'))
        formatStatus(self.getStatus(), tofd=SSD(sd))
        thesplog('STATUS: %s', sd.getvalue())


    def h_SetLogging(self, envelope):
        return self.setLoggingControls(envelope)


    def h_SystemShutdown(self, envelope):
        self._exiting = envelope.sender
        thesplog('---- shutdown initiated by %s', envelope.sender, level=logging.DEBUG)

        # Send failure notices and clear out any pending children.  If
        # any pending child ready notifications are received after
        # this, they will automatically be sent an ActorExitRequest.
        for each in self._pendingChildren:
            pendingReq = self._pendingChildren[each]
            self._send_intent(
                TransmitIntent(
                    pendingReq.sender,
                    PendingActorResponse(
                        pendingReq.message.forActor,
                        pendingReq.message.instanceNum,
                        pendingReq.message.globalName,
                        errorCode = PendingActorResponse.ERROR_ActorSystem_Shutting_Down)))
        self._pendingChildren = []

        if not self.childAddresses:  # no children?
            self._sayGoodbye()
            self.transport.abort_run(drain=True)
            return True

        # Now shutdown any direct children
        self._killLocalActors()

        # Once children confirm their exits the callback will shutdown the Admin.
        return True


    def _killLocalActors(self):
        for each in self.childAddresses:
            self._send_intent(
                TransmitIntent(each, ActorExitRequest(recursive=True),
                               onError=lambda r,m: self._handleChildExited(each)))


    def _sayGoodbye(self):
        self._cleanupAdmin()
        self._send_intent(TransmitIntent(self._exiting, SystemShutdownCompleted()))
        thesplog('---- shutdown completed', level=logging.INFO)


    def h_ChildActorExited(self, envelope):
        return self._handleChildExited(envelope.message.childAddress)


    def _handleChildExited(self, childAddress):
        if childAddress in self._nannying:
            # Let original requesting Actor (that *thinks* it's the
            # parent) know about this child exit as well.
            self._send_intent(TransmitIntent(self._nannying[childAddress],
                                             ChildActorExited(childAddress)))
            del self._nannying[childAddress]
        return super(AdminCore, self)._handleChildExited(childAddress)


    def h_PendingActor(self, envelope):
        """Admin is creating an Actor.  This covers one of the following cases:
            1. Creating for external requester via ActorSystem.
                  envelope.message.forActor will be None
               The Admin is the "parent" for all externally-created Actors.
            2. Creating for another Actor when direct creation fails.  Usually means
               that the current ActorSyste cannot meet the new Actor's requirements
               and the Admin should find another convention member that can create
               the new Actor.
            3. GlobalName Actor.  The Admin is the "parent" for all GlobalName Actors.
        """
        self._sCBStats.inc('Admin Message Received.Type.Pending Actor Request')

        if self.isShuttingDown():
            self._sendPendingActorResponse(envelope, None,
                                           errorCode = PendingActorResponse.ERROR_ActorSystem_Shutting_Down)
            return True

        sourceHash = envelope.message.sourceHash
        if sourceHash and sourceHash not in self._sources:
            self._sendPendingActorResponse(envelope, None,
                                           errorCode = PendingActorResponse.ERROR_Invalid_SourceHash)
            return True

        # Note, both Admin and remote requester will have a local
        # child address for the child (with a different instanceNumber
        # each).
        childAddr = self._addrManager.createLocalAddress()
        childInstance = childAddr.addressDetails.addressInstanceNum

        try:
            self._startChildActor(childAddr, envelope.message.actorClassName,
                                  parentAddr = self.myAddress, # Admin is surrogate parent
                                  notifyAddr = self.myAddress,
                                  childRequirements = envelope.message.targetActorReq,
                                  sourceHash = sourceHash,
                                  sourceToLoad = (self._sources[sourceHash]
                                                  if sourceHash else None))
        except NoCompatibleSystemForActor:
            self._sendPendingActorResponse(envelope, None,
                                           errorCode = PendingActorResponse.ERROR_No_Compatible_ActorSystem)
            self._retryPendingChildOperations(childInstance, None)
            return True

        self._pendingChildren[childInstance] = envelope
        # transport will contrive to call _pendingActorReady when the
        # child is initialized and connected to this parent.
        return True


    def _sendPendingActorResponse(self, requestEnvelope, actualAddress,
                                  errorCode = None, errorStr = None):
        # actualAddress is None for failure
        if actualAddress is None and errorCode is None:
            raise ValueError('Must specify either actualAddress or errorCode')
        self._send_intent(
            TransmitIntent(requestEnvelope.message.forActor or requestEnvelope.sender,
                           PendingActorResponse(requestEnvelope.message.forActor,
                                                requestEnvelope.message.instanceNum,
                                                requestEnvelope.message.globalName,
                                                errorCode = errorCode,
                                                errorStr = errorStr,
                                                actualAddress = actualAddress)))


    def _pendingActorReady(self, childInstance, actualAddress):
        if childInstance not in self._pendingChildren:
            thesplog('Pending actor is ready at %s for %s but latter is unknown'
                     '; sending child a shutdown',
                     actualAddress, childInstance, level=logging.WARNING)
            self._send_intent(
                TransmitIntent(actualAddress, ActorExitRequest(recursive=True)))
            return

        requestEnvelope = self._pendingChildren[childInstance]
        del self._pendingChildren[childInstance]
        if requestEnvelope.message.globalName or \
           not requestEnvelope.message.forActor:
            # The Admin is the responsible Parent for these children
            self._registerChild(actualAddress)
        else:
            # Anything the Admin was requested to create is a adoptive
            # child and should be killed when the Admin exits.
            self._registerChild(actualAddress)

        if requestEnvelope.message.forActor:
            # Proxy-parenting; remember the real parent
            self._nannying[actualAddress] = requestEnvelope.message.forActor
        self._addrManager.associateUseableAddress(self.myAddress, childInstance, actualAddress)
        # n.b. childInstance is for this Admin, but caller's childInstance is in original request
        self._sendPendingActorResponse(requestEnvelope, actualAddress)
        self._retryPendingChildOperations(childInstance, actualAddress)


    def h_HandleDeadLetters(self, envelope):  # handlerAddr, enableHandler
        self._sCBStats.inc('Admin Message Received.Type.Set Dead Letter Handler')
        if envelope.message.enableHandler:
            self._deadLetterHandler = envelope.message.handlerAddr
        else:
            if self._deadLetterHandler == envelope.message.handlerAddr:
                self._deadLetterHandler = None
        return True


    def h_DeadEnvelope(self, envelope):
        self._sCBStats.inc('Admin Message Received.Type.Dead Letter')
        if self._deadLetterHandler:
            self._send_intent(
                TransmitIntent(self._deadLetterHandler,
                               envelope.message))
        return True


    def h_RegisterSourceAuthority(self, envelope):
        self._sourceAuthority = envelope.message.authorityAddress


    def h_ValidateSource(self, envelope):
        sourceHash = envelope.message.sourceHash
        if not envelope.message.sourceData:
            self.unloadActorSource(sourceHash)
            logging.getLogger('Thespian').info('Source hash %s unloaded', sourceHash)
            return
        if sourceHash in self._sources:
            logging.getLogger('Thespian').info('Source hash %s already loaded', sourceHash)
            return
        if self._sourceAuthority:
            self._send_intent(TransmitIntent(self._sourceAuthority, envelope.message))
            return
        # Any attempt to load sources is ignored if there is no active
        # Source Authority.  This is a security measure to protect the
        # un-protected.
        logging.getLogger('Thespian').warning(
            'No source authority to validate source hash %s',
            sourceHash)

    def h_ValidatedSource(self, envelope):
        self._loadValidatedActorSource(envelope.message.sourceHash,
                                       envelope.message.sourceZip)
        thesplog('Source hash %s validated by Source Authority; now available.',
                 envelope.message.sourceHash)

    def _loadValidatedActorSource(self, sourceHash, sourceZip):
        # Validate the source file; this doesn't actually utilize the
        # sourceZip, but it ensures that the sourceZip isn't garbage
        # before registering it as active source.
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

    def unloadActorSource(self, sourceHash):
        if sourceHash in self._sources:
            del self._sources[sourceHash]
        for pnum, metapath in enumerate(sys.meta_path):
            if getattr(metapath, 'srcHash', None) == sourceHash:
                rmmods = [M for M in sys.modules if M.startswith(metapath.hashRoot())]
                for each in rmmods:
                    del sys.modules[each]
                del sys.meta_path[pnum]
                break


    def h_CapabilityUpdate(self, envelope):
        if self._updSystemCapabilities(envelope.message.capabilityName,
                                       envelope.message.capabilityValue):
            self._capUpdateLocalActors()

    def _updSystemCapabilities(self, cName, cVal):
        updateLocals = False
        if cVal is not None:
            updateLocals = cName not in self.capabilities or \
                           self.capabilities[cName] != cVal
            self.capabilities[cName] = cVal
        else:
            if cName in self.capabilities:
                updateLocals = True
                del self.capabilities[cName]
        return updateLocals

    def _capUpdateLocalActors(self):
        newCaps = NewCapabilities(self.capabilities, self.myAddress)
        for each in self.childAddresses:
            self._send_intent(TransmitIntent(each, newCaps))

