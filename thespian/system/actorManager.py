"The ActorManager is the wrapper and support structure for an actual Actor instance."

import sys
import logging
import copy
from thespian.actors import *
from thespian.system.utilis import thesplog
from thespian.system.systemCommon import systemCommonBase
from thespian.system.addressManager import CannotPickleAddress
from thespian.system.transport import *
from thespian.system.messages.admin import *
from thespian.system.messages.status import Thespian_StatusReq, Thespian_ActorStatus
from thespian.system.messages import *
from thespian.system.messages.convention import NotifyOnSystemRegistration
from thespian.system.messages.logcontrol import SetLogging
from thespian.system.utilis import ExpiryTime, actualActorClass
from thespian.system.sourceLoader import loadModuleFromHashSource
from datetime import timedelta
from functools import partial


MAX_SHUTDOWN_DRAIN_PERIOD=timedelta(seconds=7)


class ActorManager(systemCommonBase):
    def __init__(self, childClass, transport, sourceHash, sourceToLoad,
                 parentAddr, adminAddr,
                 childRequirements, currentSystemCapabilities):
        super(ActorManager, self).__init__(adminAddr, transport)
        self._parentAddr = parentAddr
        self._sourceHash = sourceHash
        self._sources    = { sourceHash: sourceToLoad }
        # Cache the current system capabilities to use for createActor
        # attempts.
        self.capabilities = currentSystemCapabilities
        self._actorClass = childClass  # nb. this may be a string, and sourceHash is not loaded yet
        self._childReqs  = childRequirements
        self.actorInst   = None
        thesplog('Starting Actor %s at %s', childClass, self.transport.myAddress,
                 level = logging.INFO, primary=True)



    def _createInstance(self):
        aClass = self._actorClass
        try:
            aClass = actualActorClass(aClass,
                                      partial(loadModuleFromHashSource,
                                              self._sourceHash,
                                              self._sources)
                                      if self._sourceHash else None)
            # Now instantiate the identified Actor class object
            actorInst = aClass()
            self._sCBStats.inc('Actor.Instance Created')
        except Exception as ex:
            import traceback
            thesplog('Actor %s @ %s instantiation exception: %s', self._actorClass,
                     self.transport.myAddress, traceback.format_exc(),
                     level=logging.ERROR, primary=True)
            self._sCBStats.inc('Actor.Instance Create Failed')
            self._sayGoodbye()
            return

        self.actorInst = actorInst
        self.actorInst._myRef = self


    def run(self):
        if self.actorInst is None: self._createInstance()
        if self.actorInst:
            try:
                self.transport.run(self.handleMessages)
                # Expects that on completion of self.transport.run
                # that the Actor is done processing and that it has
                # been shutdown gracefully.
            except Exception as ex:
                # This is usually an internal problem, since the
                # request handling itself catches any exceptions from
                # the Actor itself.
                import traceback
                thesplog('Actor %s @ %s transport run exception: %s',
                         self._actorClass, self.transport.myAddress,
                         traceback.format_exc(),
                         level=logging.ERROR, exc_info=True)
                self._shutdownActor(True)
                self.drainTransmits()
        else:
            self.drainTransmits()
        thesplog('Run %s done', self._actorClass, level=logging.DEBUG)


    def drainTransmits(self):
        drainLimit = ExpiryTime(MAX_SHUTDOWN_DRAIN_PERIOD)
        while not drainLimit.expired():
            if not self.transport.run(TransmitOnly, drainLimit.remaining()):
                break  # no transmits left


    def handleMessages(self, envelope):
        self._sCBStats.inc('Actor.Message Received.Total')
        self._addrManager.importAddr(envelope.sender)
        r = self._handleOneMessage(envelope)
        while r and self._receiveQueue:
            r = self._handleOneMessage(self._receiveQueue.pop(0))
        return r


    def _handleOneMessage(self, envelope):

        msg = envelope.message

        import logging    # Need to re-iterate this for some reason, otherwise it's not found below
        thesplog('ACTOR got %s', envelope.identify(), level = logging.DEBUG)

        handled, result = self._handleReplicatorMessages(envelope)
        if handled:
            return result

        if isinstance(msg, PendingActorResponse):
            return self._pendingActorResponse(envelope)

        if isinstance(msg, Thespian_StatusReq):
            return self.getActorStatus(envelope)

        if isinstance(msg, NewCapabilities):
            return self.checkNewCapabilities(envelope)

        if isinstance(msg, SetLogging):
            return self.setLoggingControls(envelope)

        if not getattr(self, '_exiting', False) or \
           isinstance(msg, (ActorExitRequest, ChildActorExited)):
            try:
                # n.b. no deepcopy of the message to protect against
                # Actor changes to the message.  This would be (a)
                # slower, and (b) very difficult to prevent
                # unpickleable errors for messages containing
                # LocalAddresses (e.g. ChildActorExit for a
                # non-instantiated child that still only has a local
                # address) since both copy and pickle call the
                # __getstate__ of the ActorAddress.
                self._sCBStats.inc('Actor.Message Received.To Actor')
                self.actorInst.receiveMessage(msg, envelope.sender)
            except Exception as ex:
                thesplog('Handling exception on msg "%s": %s', msg, ex, exc_info=True)
                self._sCBStats.inc('Actor.Message Received.Caused Primary Exception')
                if not isinstance(msg, ActorExitRequest):
                    import traceback
                    thesplog('Actor %s @ %s retryable exception on message %s: %s',
                             self._actorClass, self.transport.myAddress, msg,
                             traceback.format_exc(),
                             level = logging.WARNING)
                    logging.getLogger(str(self._actorClass)) \
                           .warning('Actor %s @ %s retryable exception on message %s',
                                    self._actorClass, self.transport.myAddress, msg,
                                    exc_info = True)
                    try:
                        self.actorInst.receiveMessage(copy.deepcopy(msg), envelope.sender)
                    except Exception:
                        self._sCBStats.inc('Actor.Message Received.Caused Secondary Exception')
                        thesplog('Actor %s @ %s second exception on message %s: %s',
                                 self._actorClass, self.transport.myAddress, msg,
                                 traceback.format_exc(),
                                 level = logging.ERROR)
                        logging.getLogger(str(self._actorClass)) \
                               .error('Actor %s @ %s second exception on message %s',
                                      self._actorClass, self.transport.myAddress, msg,
                                      exc_info = True)
                        self._send_intent(
                            TransmitIntent(envelope.sender, PoisonMessage(msg)))


        if isinstance(msg, ActorExitRequest):
            if getattr(self, '_exiting', None) is not None:
                return True  # multiple shutdown requests ignored
            # Initiate exit; may be a delay while children are shutdown
            return self._actorExit(msg)

        if isinstance(msg, ChildActorExited):
            return self._handleChildExited(msg.childAddress)

        return True


    def _actorExit(self, exitRequest):
        return self._shutdownActor(exitRequest.isRecursive)


    def _shutdownActor(self, shutdownChildren=True):
        children = self.childAddresses
        if shutdownChildren and children:
            for each in children:
                self._send_intent(
                    TransmitIntent(each, ActorExitRequest(recursive=True),
                                   onError=lambda r,m: self._childInaccessible(each, m)))

        else:
            self._sayGoodbye()

        self._exiting = True  # set exiting mode

        if shutdownChildren and children:
            # Should wait for children to exit before this Actor exits, but drop everything else
            return True  # keep going
        # Don't need to wait for children, so exit as soon as transmit pipe drains.
        self.transport.abort_run(drain=True)
        return True


    def _sayGoodbye(self):
        self._send_intent(
            TransmitIntent(self._parentAddr, ChildActorExited(self.transport.myAddress)))


    def _childInaccessible(self, childAddress, exitRequestIntent):
        self._handleChildExited(childAddress)


    def checkNewCapabilities(self, envelope):
        newCaps = envelope.message.newCapabilities
        if hasattr(self.actorInst, 'actorSystemCapabilityCheck') and \
           not self.actorInst.actorSystemCapabilityCheck(newCaps, self._childReqs):
            self._send_intent(TransmitIntent(self.myAddress,
                                             ActorExitRequest()))
        self.capabilities = envelope.message.newCapabilities
        for child in self.childAddresses:
            self._send_intent(TransmitIntent(child, envelope.message))
        return True



    # ----------------------------------------------------------------------
    # Transmit management

    def actor_send(self, targetAddr, msg):
        self._sCBStats.inc('Actor.Message Send.Generated')
        self._send_intent(TransmitIntent(targetAddr, msg))


    # ----------------------------------------------------------------------
    # Child Actor Management

    def createActor(self, newActorClass, targetActorRequirements, globalName,
                    sourceHash = None):
        naa = self._addrManager.createLocalAddress()
        if getattr(self, '_exiting', False):
            return naa

        if not globalName:
            try:
                self._startChildActor(naa, newActorClass, self.myAddress,
                                      notifyAddr = self.myAddress,
                                      childRequirements = targetActorRequirements,
                                      sourceHash = sourceHash or self._sourceHash)
                # transport will contrive to call _pendingActorReady when the
                # child is initialized and connected to this parent.
                return naa
            except NoCompatibleSystemForActor:
                pass

        # Cannot create the actor directly, so ask the Admin for help
        actorClassName = '%s.%s'%(newActorClass.__module__,
                                  newActorClass.__name__) if hasattr(newActorClass, '__name__') else newActorClass
        self._send_intent(
            TransmitIntent(self._adminAddr,
                           PendingActor(actorClassName,
                                        self.transport.myAddress,
                                        naa.addressDetails.addressInstanceNum,
                                        targetActorRequirements,
                                        globalName=globalName,
                                        sourceHash=sourceHash or self._sourceHash)))
        return naa


    def _pendingActorReady(self, childInstance, actualAddress, isMyChild=True):
        self._addrManager.associateUseableAddress(self.myAddress,
                                                  childInstance,
                                                  actualAddress)
        if isMyChild: self._registerChild(actualAddress)

        # Send any queued transmits for this child, and move the
        # finalTransmit marker to use the new address.

        self._retryPendingChildOperations(childInstance, actualAddress)


    def _pendingActorResponse(self, envelope):
        # Have seen it arrive here without errorCode set on the PendingActorResponse...
        if not hasattr(envelope.message, 'errorCode'):
            thesplog('Corrupted Pending Actor Response?: %s (%s)',
                     envelope.message, dir(envelope.message), level = logging.ERROR)
            return True
        if not getattr(envelope.message, 'errorCode', 'Failed'):
            self._pendingActorReady(envelope.message.instanceNum,
                                    envelope.message.actualAddress,
                                    isMyChild = not envelope.message.globalName)
            return True
        # Pending Actor Creation failed, clean up all the stuff associated with the intended Actor
        self._retryPendingChildOperations(envelope.message.instanceNum, None)
        return True


    def getActorStatus(self, envelope):
        self._sCBStats.inc('Actor.Message Received.Status Request')
        resp = Thespian_ActorStatus(self.myAddress,
                                    self._actorClass,
                                    self._adminAddr,
                                    self._parentAddr,
                                    self._sourceHash)
        self._updateStatusResponse(resp)
        self._send_intent(TransmitIntent(envelope.sender, resp))
        return True


    # ----------------------------------------------------------------------
    # Actor System Interaction

    def handleDeadLetters(self, address, enable):
        self._send_intent(
            TransmitIntent(self._adminAddr, HandleDeadLetters(address, enable)))


    def notifyOnSystemRegistrationChanges(self, watcherAddress, enable):
        self._send_intent(
            TransmitIntent(self._adminAddr,
                           NotifyOnSystemRegistration(watcherAddress, enable)))


    def updateCapability(self, capabilityName, capabilityValue):
        self._send_intent(
            TransmitIntent(self._adminAddr,
                           CapabilityUpdate(capabilityName, capabilityValue)))


    def registerSourceAuthority(self, address):
        self._send_intent(
            TransmitIntent(self._adminAddr, RegisterSourceAuthority(address)))


    # ----------------------------------------------------------------------
    # Miscellaneous Functionality

    def wakeupAfter(self, timePeriod):
        self.transport.addWakeup(timePeriod)
