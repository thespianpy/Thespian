"The ActorManager is the wrapper and support structure for an actual Actor instance."

import sys
import logging
import copy
import atexit
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
from thespian.system.utilis import actualActorClass
from thespian.system.timing import ExpiryTime
from thespian.system.sourceLoader import loadModuleFromHashSource
from datetime import timedelta
from functools import partial


MAX_SHUTDOWN_DRAIN_PERIOD=timedelta(seconds=7)


class ActorManager(systemCommonBase):
    def __init__(self, childClass, transport, sourceHash, sourceToLoad,
                 parentAddr, adminAddr,
                 childRequirements, currentSystemCapabilities,
                 concurrency_context):
        super(ActorManager, self).__init__(adminAddr, transport)
        self.init_replicator(transport, concurrency_context)
        self._parentAddr = parentAddr
        self._sourceHash = sourceHash
        self._sources    = { sourceHash: sourceToLoad }
        # Cache the current system capabilities to use for createActor
        # attempts.
        self.capabilities = currentSystemCapabilities
        self._actorClass = childClass  # nb. this may be a string, and sourceHash is not loaded yet
        self._childReqs  = childRequirements
        self.actorInst   = None
        atexit.register(self._shutdownActor)
        thesplog('Starting Actor %s at %s (parent %s, admin %s)',
                 childClass, self.transport.myAddress,
                 self._parentAddr, adminAddr,
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
            logging.getLogger(str(self._actorClass)) \
                   .error('Actor %s @ %s instantiation exception',
                          self._actorClass, self.transport.myAddress,
                          exc_info = True)
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
                while True:
                    r = self.transport.run(self.handleMessages)
                    if isinstance(r, Thespian__UpdateWork):
                        self._send_intent(
                            TransmitIntent(self.myAddress, r))  # tickle the transmit queues
                        continue
                    # Expects that on completion of self.transport.run
                    # that the Actor is done processing and that it has
                    # been shutdown gracefully.
                    break
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
            return self.replyWithActorStatus(envelope)

        if isinstance(msg, NewCapabilities):
            return self.checkNewCapabilities(envelope)

        if isinstance(msg, SetLogging):
            return self.setLoggingControls(envelope)

        actor_result = None
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
                actor_result = self.actorInst.receiveMessage(msg, envelope.sender)
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
                        actor_result = self.actorInst.receiveMessage(copy.deepcopy(msg), envelope.sender)
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
                        if not isinstance(msg, PoisonMessage):
                            self._send_intent(
                                TransmitIntent(
                                    envelope.sender,
                                    PoisonMessage(msg,
                                                  traceback.format_exc())))


        if isinstance(msg, ActorExitRequest):
            return self._actorExit(msg)

        if hasattr(self.transport, 'set_watch'):
            self.transport.set_watch(actor_result.filenos if isinstance(actor_result, ThespianWatch) else [])
        else:
            if isinstance(actor_result, ThespianWatch):
                logging.getLogger(str(self._actorClass))\
                       .error('Actor %s @ %s does not support ThespianWatch',
                              self._actorClass,
                              self.transport.myAddress)

        if isinstance(msg, ChildActorExited):
            return self._handleChildExited(msg.childAddress)

        return True


    def _actorExit(self, exitRequest):
        return self._shutdownActor(exitRequest.isRecursive)


    def _shutdownActor(self, shutdownChildren=True):
        if hasattr(atexit, 'unregister'):
            atexit.unregister(self._shutdownActor)
        if getattr(self, '_exiting', None):
            return True  # already exiting
        self._exiting = True  # set exiting mode

        children = self.childAddresses
        if shutdownChildren and children:
            for each in children:
                self._send_intent(
                    TransmitIntent(each, ActorExitRequest(recursive=True),
                                   onError=lambda r,m: self._childInaccessible(each, m)))

        else:
            self._sayGoodbye()

        if shutdownChildren and children:
            # Should wait for children to exit before this Actor exits, but drop everything else
            return True  # keep going
        # Don't need to wait for children, so exit as soon as transmit pipe drains.
        self.transport.abort_run(drain=True)
        return True


    def _sayGoodbye(self):
        self._send_intent(
            TransmitIntent(self._adminAddr,
                           NotifyOnSourceAvailability(self.transport.myAddress,
                                                      False)))
        self._send_intent(
            TransmitIntent(self._parentAddr,
                           ChildActorExited(self.transport.myAddress)))


    def _childInaccessible(self, childAddress, exitRequestIntent):
        self._handleChildExited(childAddress)


    def checkNewCapabilities(self, envelope):
        newCaps = envelope.message.newCapabilities
        if envelope.message.adminAddress == self._adminAddr:
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
        self._send_intent(TransmitIntent(targetAddr, msg,
                                         onError=self.actor_send_fail))

    def actor_send_fail(self, result, intent):
        # If this was a DeadTarget failure, forward to the Admin for
        # dead letter handling (with appropriate avoiding of recursion
        # loops; see also addressManager.py:prepMessageSend).
        if result == SendStatus.DeadTarget and \
           intent.targetAddr != self._adminAddr and \
           not isinstance(intent.message, (DeadEnvelope, ChildActorExited)):
            self._send_intent(TransmitIntent(self._adminAddr,
                                             DeadEnvelope(intent.targetAddr,
                                                          intent.message)))


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
            except ImportError:  # hash source may not be available locally
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
        thesplog('Pending Actor create failed (%s): %s',
                 getattr(envelope.message, 'errorCode', '??'),
                 getattr(envelope.message, 'errorStr', '---'))
        logging.getLogger(str(self._actorClass)) \
               .error('Pending Actor create failed (%s): %s',
                      getattr(envelope.message, 'errorCode', '??'),
                      getattr(envelope.message, 'errorStr', '---'))
        self._retryPendingChildOperations(envelope.message.instanceNum, None)
        return True


    def getActorStatus(self):
        resp = Thespian_ActorStatus(self.myAddress,
                                    self._actorClass,
                                    self._adminAddr,
                                    self._parentAddr,
                                    self._sourceHash,
                                    getattr(self, '_exiting', None))
        self._updateStatusResponse(resp)
        return resp

    def replyWithActorStatus(self, envelope):
        self._sCBStats.inc('Actor.Message Received.Status Request')
        self._send_intent(TransmitIntent(envelope.sender,
                                         self.getActorStatus()))
        return True

    def thesplogStatus(self):
        "Write status to thesplog"
        from io import StringIO
        sd = StringIO()
        try:
            sd.write('')
            SSD = lambda v: v
        except TypeError:
            class SSD(object):
                def __init__(self, sd): self.sd = sd
                def write(self, str_arg): self.sd.write(str_arg.decode('utf-8'))
        from thespian.system.messages.status import formatStatus
        formatStatus(self.getActorStatus(), tofd=SSD(sd))
        thesplog('STATUS: %s', sd.getvalue())


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


    def notifyOnSourceAvailability(self, watcherAddress, enable):
        self._send_intent(
            TransmitIntent(self._adminAddr,
                           NotifyOnSourceAvailability(watcherAddress, enable)))


    def loadActorSource(self, fname):
        f = fname if hasattr(fname, 'read') else open(fname, 'rb')
        try:
            d = f.read()
            import hashlib
            hval = hashlib.md5(d).hexdigest()
            self._send_intent(
                TransmitIntent(self._adminAddr,
                               ValidateSource(hval, d,
                                              getattr(f, 'name',
                                                      str(fname)
                                                      if hasattr(fname, 'read')
                                                      else fname))))
            return hval
        finally:
            f.close()


    def unloadActorSource(self, sourceHash):
        self._send_intent(TransmitIntent(self._adminAddr,
                                         ValidateSource(sourceHash, None)))


    # ----------------------------------------------------------------------
    # Miscellaneous Functionality

    def wakeupAfter(self, timePeriod):
        self.transport.addWakeup(timePeriod)

    # ----------------------------------------------------------------------
    # Actors that involve themselves in topology

    def preRegisterRemoteSystem(self, remoteAddress, remoteCapabilities):
        from thespian.system.messages.convention import ConventionRegister
        self._send_intent(
            TransmitIntent(
                self._adminAddr,
                ConventionRegister(
                    self.transport.getAddressFromString(remoteAddress),
                    remoteCapabilities,
                    preRegister=True)))

    def deRegisterRemoteSystem(self, remoteAddress):
        from thespian.system.messages.convention import ConventionDeRegister
        self._send_intent(
            TransmitIntent(self._adminAddr,
                           ConventionDeRegister(
                               remoteAddress
                               if isinstance(remoteAddress, ActorAddress) else
                               self.transport.getAddressFromString(remoteAddress),
                               preRegistered=True)))


    def actorSystemShutdown(self):
        self._send_intent(TransmitIntent(self._adminAddr, SystemShutdown()))
