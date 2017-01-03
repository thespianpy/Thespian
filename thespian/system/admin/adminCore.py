import sys
import logging
from datetime import timedelta

from thespian.actors import *
from thespian.system.utilis import thesplog, AssocList
from thespian.system.systemCommon import systemCommonBase
from thespian.system.messages.status import Thespian_SystemStatus
from thespian.system.messages.admin import *
from thespian.system.transport import TransmitIntent
from thespian.system.sourceLoader import SourceHashFinder
from thespian.system.timing import ExpirationTimer



SOURCE_LOAD_TIMEOUT_PERIOD = timedelta(minutes=2)


class PendingSource(object):
    source_valid = False
    def __init__(self, srchash, orig_data):
        self.srchash = srchash
        self.orig_data = orig_data
        self.load_expires = ExpirationTimer(SOURCE_LOAD_TIMEOUT_PERIOD)
        # pending_actors is an array of PendingActor requests waiting
        # on this source.
        self.pending_actors = []

class ValidSource(object):
    source_valid = True
    def __init__(self, srchash, orig_data, zipsrc, srcinfo):
        self.srcHash = srchash
        self.orig_data = orig_data
        self.zipsrc  = zipsrc
        self.srcInfo = srcinfo


class AdminCore(systemCommonBase):

    def __init__(self, transport, address, capabilities,
                 logdefs,
                 concurrency_context):
        thesplog('++++ Starting Admin from %s',
                 sys.modules['thespian'].__file__,
                 level=logging.DEBUG)
        super(AdminCore, self).__init__(address, transport)
        self.init_replicator(transport, concurrency_context)
        self.capabilities = capabilities
        self.logdefs = logdefs
        self._pendingChildren = {}  # Use: childLocalAddr instance # : PendingActorEnvelope
        # Things that help us look like an Actor, even though we're not
        self._sourceHash = None
        thesplog('++++ Admin started @ %s / gen %s',
                 self.transport.myAddress, str(ThespianGeneration),
                 level=logging.INFO,
                 primary=True)
        logging.info('++++ Actor System gen %s started, admin @ %s',
                     str(ThespianGeneration), self.transport.myAddress)
        logging.debug('Thespian source: %s', sys.modules['thespian'].__file__)
        self._nannying = AssocList()  # child actorAddress -> parent Address
        self._deadLetterHandler = None
        self._sources = {}  # Index is sourcehash, value PendingSource or ValidSource
        self._sourceAuthority = None
        self._sourceNotifications = []  # array of notification addresses


    def _activate(self):
        """Called when the full ActorSystem initialization is completed.  This
           should then activate any functionality that needed to wait
           for completion of initialization.
        """
        pass


    def run(self):
        try:
            while True:
                r = self.transport.run(self.handleIncoming, None)
                if isinstance(r, Thespian__UpdateWork):
                    # tickle the transmit queues
                    self._send_intent(
                        TransmitIntent(self.myAddress, r))
                    continue
                # Expects that on completion of self.transport.run
                # that the Actor is done processing and that it has
                # been shutdown gracefully.
                break
        except Exception:
            import traceback
            thesplog('ActorAdmin uncaught exception: %s',
                     traceback.format_exc(),
                     level=logging.ERROR, exc_info=True)
        thesplog('Admin time to die', level=logging.DEBUG)


    def handleIncoming(self, envelope):
        self._sCBStats.inc('Admin Message Received.Total')
        handled, result = self._handleReplicatorMessages(envelope)
        if handled:
            return result
        if isinstance(envelope.message, (ActorSystemMessage,
                                         logging.LogRecord)):
            thesplog('Admin of %s', envelope.identify(), level=logging.DEBUG)
            return getattr(self,
                           'h_' + envelope.message.__class__.__name__,
                           self.unrecognized)(envelope)
        # else discard random non-admin messages
        self._sCBStats.inc('Admin Message Received.Ignored')
        thesplog('ADMIN DISCARD %s', envelope.identify(),
                 level=logging.WARNING)
        return True


    def unrecognized(self, envelope):
        self._sCBStats.inc('Admin Message Received.Discarded')
        thesplog("Admin got incoming %s from %s;"
                 " discarded because I don't know how to handle it!",
                 envelope.message, envelope.sender,
                 level=logging.WARNING, primary=True)
        return True


    def isShuttingDown(self): return hasattr(self, '_exiting')


    def h_QueryExists(self, envelope):
        self._sCBStats.inc('Admin Message Received.Type.QueryExists')
        self._send_intent(
            TransmitIntent(
                envelope.sender,
                QueryAck(
                    self.capabilities.get('Thespian ActorSystem Name',
                                          'misc Actor System'),
                    self.capabilities.get('Thespian ActorSystem Version',
                                          'unknown Version'),
                    self.isShuttingDown())))
        return True


    def getStatus(self):
        resp = Thespian_SystemStatus(self.myAddress,
                                     capabilities=self.capabilities,
                                     inShutdown=self.isShuttingDown())
        resp.setDeadLetterHandler(self._deadLetterHandler)
        self._updateStatusResponse(resp)
        resp.setLoadedSources(self._sources)
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
        thesplog('---- shutdown initiated by %s', envelope.sender,
                 level=logging.DEBUG)

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
                        errorCode=PendingActorResponse.ERROR_ActorSystem_Shutting_Down)))
        self._pendingChildren = []

        if not self.childAddresses:  # no children?
            self._sayGoodbye()
            self.transport.abort_run(drain=True)
            return True

        # Now shutdown any direct children
        self._killLocalActors()

        # Callback will shutdown the Admin Once the children confirm
        # their exits.
        return True


    def _remove_expired_sources(self):
        rmvlist = []
        for each in self._sources:
            if not self._sources[each].source_valid and \
               self._sources[each].load_expires.expired():
                rmvlist.append(each)
        for each in rmvlist:
            self._cancel_pending_actors(self._sources[each].pending_actors)
            del self._sources[each]

    def _cancel_pending_actors(self, pending_envelopes,
                               error_code=PendingActorResponse.ERROR_Invalid_SourceHash):
        for each in pending_envelopes:
            self._sendPendingActorResponse(each, None, errorCode = error_code)



    def _killLocalActors(self):
        for each in self.childAddresses:
            self._send_intent(
                TransmitIntent(each, ActorExitRequest(recursive=True),
                               onError=lambda r, m, a=each:
                               self._handleChildExited(a)))


    def _sayGoodbye(self):
        self._cleanupAdmin()
        self._send_intent(TransmitIntent(self._exiting,
                                         SystemShutdownCompleted()))
        thesplog('---- shutdown completed', level=logging.INFO)
        logging.info('---- Actor System shutdown')
        self.shutdown_completed = True


    def h_ChildActorExited(self, envelope):
        return self._handleChildExited(envelope.message.childAddress)


    def _handleChildExited(self, childAddress):
        self._sourceNotifications = list(filter(lambda a: a != childAddress,
                                                self._sourceNotifications))
        parentAddr = self._nannying.find(childAddress)
        if parentAddr:
            self._nannying.rmv(childAddress)
            # Let original requesting Actor (that *thinks* it's the
            # parent) know about this child exit as well.
            self._send_intent(TransmitIntent(parentAddr,
                                             ChildActorExited(childAddress)))
        return super(AdminCore, self)._handleChildExited(childAddress)


    def h_PendingActor(self, envelope):
        """Admin is creating an Actor.  This covers one of the following cases:

            1. Creating for external requester via ActorSystem.
                  envelope.message.forActor will be None
               The Admin is the "parent" for all externally-created Actors.

            2. Creating for another Actor when direct creation fails.
               Usually means that the current ActorSyste cannot meet
               the new Actor's requirements and the Admin should find
               another convention member that can create the new
               Actor.

            3. GlobalName Actor.  The Admin is the "parent" for all
               GlobalName Actors.

        """
        self._sCBStats.inc('Admin Message Received.Type.Pending Actor Request')

        if self.isShuttingDown():
            self._sendPendingActorResponse(
                envelope, None,
                errorCode=PendingActorResponse.ERROR_ActorSystem_Shutting_Down)
            return True

        sourceHash = envelope.message.sourceHash
        if sourceHash:
            self._remove_expired_sources()
            if sourceHash not in self._sources:
                self._sendPendingActorResponse(
                    envelope, None,
                    errorCode = PendingActorResponse.ERROR_Invalid_SourceHash)
                return True
            if not self._sources[sourceHash].source_valid:
                self._sources[sourceHash].pending_actors.append(envelope)
                return True

        # Note, both Admin and remote requester will have a local
        # child address for the child (with a different instanceNumber
        # each).
        childAddr = self._addrManager.createLocalAddress()
        childInstance = childAddr.addressDetails.addressInstanceNum

        try:
            self._startChildActor(
                childAddr, envelope.message.actorClassName,
                parentAddr=self.myAddress,  # Admin is surrogate parent
                notifyAddr=self.myAddress,
                childRequirements=envelope.message.targetActorReq,
                sourceHash=sourceHash,
                sourceToLoad=(self._sources[sourceHash]
                              if sourceHash else None))
        except NoCompatibleSystemForActor:
            self._sendPendingActorResponse(
                envelope, None,
                errorCode=PendingActorResponse.ERROR_No_Compatible_ActorSystem)
            self._retryPendingChildOperations(childInstance, None)
            return True

        self._pendingChildren[childInstance] = envelope
        # transport will contrive to call _pendingActorReady when the
        # child is initialized and connected to this parent.
        return True


    def _sendPendingActorResponse(self, requestEnvelope, actualAddress,
                                  errorCode=None, errorStr=None):
        # actualAddress is None for failure
        if actualAddress is None and errorCode is None:
            raise ValueError('Must specify either actualAddress or errorCode')
        self._send_intent(
            TransmitIntent(
                requestEnvelope.message.forActor or requestEnvelope.sender,
                PendingActorResponse(requestEnvelope.message.forActor,
                                     requestEnvelope.message.instanceNum,
                                     requestEnvelope.message.globalName,
                                     errorCode=errorCode,
                                     errorStr=errorStr,
                                     actualAddress=actualAddress)))


    def _pendingActorReady(self, childInstance, actualAddress):
        if childInstance not in self._pendingChildren:
            thesplog('Pending actor is ready at %s for UNKNOWN %s'
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
            self._nannying.add(actualAddress, requestEnvelope.message.forActor)
        self._addrManager.associateUseableAddress(self.myAddress,
                                                  childInstance,
                                                  actualAddress)
        # n.b. childInstance is for this Admin, but caller's
        # childInstance is in original request
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


    def h_NotifyOnSourceAvailability(self, envelope):
        address = envelope.message.notificationAddress
        enable = envelope.message.enable
        all_except = [A for A in self._sourceNotifications if A != address]
        if enable:
            self._sourceNotifications = all_except + [address]
            for each in self._sources:
                if self._sources[each].source_valid:
                    self._send_intent(
                        TransmitIntent(address,
                                       LoadedSource(self._sources[each].srcHash,
                                                    self._sources[each].srcInfo)))
        else:
            self._sourceNotifications = all_except


    def h_ValidateSource(self, envelope):
        self._remove_expired_sources()
        sourceHash = envelope.message.sourceHash
        if not envelope.message.sourceData:
            self.unloadActorSource(sourceHash)
            logging.getLogger('Thespian')\
                   .info('Source hash %s unloaded', sourceHash)
            return
        if sourceHash in self._sources and \
           self._sources[sourceHash].source_valid:
            logging.getLogger('Thespian')\
                   .info('Source hash %s (%s) already loaded', sourceHash,
                         self._sources[sourceHash].srcInfo
                         if isinstance(self._sources[sourceHash], ValidSource)
                         else '<pending>')
            return
        if self._sourceAuthority:
            self._sources[sourceHash] = PendingSource(sourceHash,
                                                      envelope.message.sourceData)
            self._send_intent(TransmitIntent(self._sourceAuthority,
                                             envelope.message))
            return
        # Any attempt to load sources is ignored if there is no active
        # Source Authority.  This is a security measure to protect the
        # un-protected.
        logging.getLogger('Thespian').warning(
            'No source authority to validate source hash %s',
            sourceHash)

    def h_ValidatedSource(self, envelope):
        self._remove_expired_sources()
        if envelope.sender != self._sourceAuthority:
            logging.getLogger('Thespian').warning(
                'Ignoring validated source from %s: not the source authority at %s',
                envelope.sender, self._sourceAuthority)
            return
        source_hash = envelope.message.sourceHash
        if envelope.message.sourceZip:
            self._loadValidatedActorSource(
                source_hash,
                envelope.message.sourceZip,
                getattr(envelope.message, 'sourceInfo', None))
            logging.getLogger('Thespian').info(
                'Source hash %s (%s) validated by Source Authority'
                '; now available.',
                source_hash,
                getattr(envelope.message, 'sourceInfo', '-'))
        else:
            # Source Authority actively rejected this source, so
            # actively unloaded it.  Alternatively the Source
            # Authority can do nothing and this load attempt will
            # timeout.
            self._cancel_pending_actors(
                self._sources[source_hash].pending_actors)
            logging.getLogger('Thespian').warning(
                'Source hash %s (%s) REJECTED by Source Authority',
                source_hash,
                getattr(envelope.message, 'sourceInfo', '-'))
            del self._sources[source_hash]

    def _loadValidatedActorSource(self, sourceHash, sourceZip, sourceInfo):
        # Validate the source file; this doesn't actually utilize the
        # sourceZip, but it ensures that the sourceZip isn't garbage
        # before registering it as active source.
        if sourceHash not in self._sources:
            logging.getLogger('Thespian').warning(
                'Provided validated source with no or expired request'
                ', hash %s; ignoring.', sourceHash)
            return

        try:
            f = SourceHashFinder(sourceHash, lambda v: v, sourceZip)
            namelist = f.getZipNames()
            logging.getLogger('Thespian').info(
                'Validated source hash %s - %s, %s modules (%s)',
                sourceHash, sourceInfo, len(namelist),
                ', '.join(namelist if len(namelist) < 10 else
                          namelist[:9] + ['...']))
        except Exception as ex:
            logging.getLogger('Thespian')\
                   .error('Validated source (hash %s) is corrupted: %s',
                          sourceHash, ex)
            return

        if self._sources[sourceHash].source_valid:
            # If a duplicate source load request is made while the
            # first is still being validated by the Source Authority,
            # another request will be sent to the Source Authority and
            # the latter response will be a duplicate here and can
            # simply be dropped.
            return

        # Store this registered source
        pending_actors = self._sources[sourceHash].pending_actors

        self._sources[sourceHash] = ValidSource(sourceHash,
                                                self._sources[sourceHash].orig_data,
                                                sourceZip,
                                                str(sourceInfo))

        for each in pending_actors:
            self.h_PendingActor(each)

        msg = LoadedSource(self._sources[sourceHash].srcHash,
                           self._sources[sourceHash].srcInfo)
        for each in self._sourceNotifications:
            self._send_intent(TransmitIntent(each, msg))


    def unloadActorSource(self, sourceHash):
        if sourceHash in self._sources:
            msg = UnloadedSource(self._sources[sourceHash].srcHash,
                                 self._sources[sourceHash].srcInfo)
            for each in self._sourceNotifications:
                self._send_intent(TransmitIntent(each, msg))
            del self._sources[sourceHash]
        for pnum, metapath in enumerate(sys.meta_path):
            if getattr(metapath, 'srcHash', None) == sourceHash:
                rmmods = [M for M in sys.modules
                          if M and M.startswith(metapath.hashRoot())]
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
