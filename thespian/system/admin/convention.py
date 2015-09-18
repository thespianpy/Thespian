import logging
from thespian.actors import *
from thespian.system.utilis import (thesplog, ExpiryTime, checkActorCapabilities,
                                    partition, foldl,
                                    actualActorClass)
from thespian.system.admin.globalNames import GlobalNamesAdmin
from thespian.system.transport import TransmitIntent, SendStatus
from thespian.system.messages.admin import PendingActorResponse
from thespian.system.messages.convention import *
from thespian.system.sourceLoader import loadModuleFromHashSource
from functools import partial
from datetime import datetime, timedelta


CONVENTION_REREGISTRATION_PERIOD  = timedelta(minutes=7, seconds=22)
CONVENTION_RESTART_PERIOD         = timedelta(minutes=3, seconds=22)
CONVENTION_REGISTRATION_MISS_MAX  = 3  # # of missing convention registrations before death declared
HYSTERESIS_MIN_PERIOD  = timedelta(milliseconds=250)
HYSTERESIS_MAX_PERIOD  = timedelta(seconds=45)
HYSTERESIS_RATE        = 2


class HysteresisDelaySender(object):
    """Implements hysteresis delay for sending messages.  This is intended
       to be used for messages exchanged between convention members to
       ensure that a mis-behaved member doesn't have the ability to
       inflict damage on the entire convention.  The first time a
       message is sent via this sender it is passed on through, but
       that starts a blackout period that starts with the
       CONVENTION_HYSTERESIS_MIN_PERIOD.  Each additional send attempt
       during that blackout period will cause the blackout period to
       be extended by the CONVENTION_HYSTERESIS_RATE, up to the
       CONVENTION_HYSTERESIS_MAX_PERIOD.  Once the blackout period
       ends, the queued sends will be sent, but only the last
       attempted message of each type for the specified remote target.
       At that point, the hysteresis delay will be reduced by the
       CONVENTION_HYSTERESIS_RATE; further send attempts will affect
       the hysteresis blackout period as described as above but lack
       of sending attempts will continue to reduce the hysteresis back
       to a zero-delay setting.

       Note: delays are updated in a target-independent manner; the
             target is only considered when eliminating duplicates.

       Note: maxDelay on TransmitIntents is ignored by hysteresis
             delays.  It is assumed that a transmit intent's maxDelay
             is greater than the maximum hysteresis period and/or that
             the hysteresis delay is more important than the transmit
             intent timeout.
    """
    def __init__(self, actual_sender,
                 hysteresis_min_period = HYSTERESIS_MIN_PERIOD,
                 hysteresis_max_period = HYSTERESIS_MAX_PERIOD,
                 hysteresis_rate       = HYSTERESIS_RATE):
        self._sender                = actual_sender
        self._hysteresis_until      = ExpiryTime(timedelta(seconds=0))
        self._hysteresis_queue      = []
        self._duplicate_queue       = []
        self._current_hysteresis    = None  # timedelta
        self._hysteresis_min_period = hysteresis_min_period
        self._hysteresis_max_period = hysteresis_max_period
        self._hysteresis_rate       = hysteresis_rate
    @property
    def delay(self): return self._hysteresis_until
    def checkSends(self):
        if self.delay.expired():
            hsends = self._hysteresis_queue
            self._hysteresis_queue = []
            self._current_hysteresis = (
                None
                if (self._current_hysteresis is None or
                    self._current_hysteresis < self._hysteresis_min_period) else
                self._current_hysteresis / self._hysteresis_rate)
            self._hysteresis_until = ExpiryTime(self._current_hysteresis
                                                if self._current_hysteresis else
                                                timedelta(seconds=0))
            for intent in hsends:
                self._sender(intent)
    def sendWithHysteresis(self, intent):
        if self._hysteresis_until.expired():
            self._current_hysteresis = self._hysteresis_min_period
            self._sender(intent)
        else:
            dups = self._keepIf(lambda M: (M.targetAddr != intent.targetAddr or
                                                 type(M.message) != type(intent.message)))
            # The dups are duplicate sends to the new intent's target; complete them when
            # the actual message is finally sent with the same result
            if dups:
                intent.addCallback(self._dupSentGood(dups), self._dupSentFail(dups))
            self._hysteresis_queue.append(intent)
            self._current_hysteresis = min(
                (self._hysteresis_min_period
                 if (self._current_hysteresis is None or
                     self._current_hysteresis < self._hysteresis_min_period) else
                 self._current_hysteresis * self._hysteresis_rate),
                self._hysteresis_max_period)
        self._hysteresis_until = ExpiryTime(
            timedelta(seconds=0)
            if not self._current_hysteresis else
            (self._current_hysteresis -
             (timedelta(seconds=0)
              if not self._hysteresis_until else
              self._hysteresis_until.remaining())))
    def cancelSends(self, remoteAddr):
        cancels = self._keepIf(lambda M: M.targetAddr != remoteAddr)
        for each in cancels:
            each.result = SendStatus.Failed
            each.completionCallback()
    def _keepIf(self, keepFunc):
        requeues, removes = partition(keepFunc, self._hysteresis_queue)
        self._hysteresis_queue = requeues
        return removes
    @staticmethod
    def _dupSentGood(dups):
        def _finishDups(result, finishedIntent):
            for each in dups:
                each.result = result
                each.completionCallback()
        return _finishDups
    @staticmethod
    def _dupSentFail(dups):
        def _finishDups(result, finishedIntent):
            for each in dups:
                each.result = result
                each.completionCallback()
        return _finishDups




class ConventionMemberData(object):
    def __init__(self, address, capabilities):
        self.remoteAddress      = address
        self.remoteCapabilities = capabilities
        self.registryValid      = ExpiryTime(CONVENTION_REREGISTRATION_PERIOD *
                                             CONVENTION_REGISTRATION_MISS_MAX)
        self.hasRemoteActors    = []  # (localParent, remoteActor) addresses created remotely
        self.lastMessaged       = None # datetime of access; use with CONVENTION_HYSTERESIS_PERIOD
    def createdActor(self, localParentAddress, newActorAddress):
        entry = localParentAddress, newActorAddress
        if entry not in self.hasRemoteActors:
            self.hasRemoteActors.append(entry)

    def __str__(self):
        return 'ActorSystem @ %s, registry valid for %s with %s'%(str(self.remoteAddress),
                                                                  str(self.registryValid),
                                                                  str(self.remoteCapabilities))


class ConventioneerAdmin(GlobalNamesAdmin):
    """Extends the AdminCore+GlobalNamesAdmin with ActorSystem Convention
       functionality to support multi-host configurations.
    """
    def __init__(self, *args, **kw):
        super(ConventioneerAdmin, self).__init__(*args, **kw)
        self._conventionMembers = {} # key=Remote Admin Addr, value=ConventionMemberData
        self._conventionRegistration = ExpiryTime(timedelta(seconds=0))
        self._conventionNotificationHandler = None
        self._conventionAddress = None  # Not a member; still could be a leader
        self._pendingSources = {}  # key = sourceHash, value is array of PendingActor requests
        self._hysteresisSender = HysteresisDelaySender(self._send_intent)

    def _updateStatusResponse(self, resp):
        resp.setConventionLeaderAddress(self._conventionAddress)
        resp.setConventionRegisterTime(self._conventionRegistration)
        for each in self._conventionMembers:
            resp.addConventioneer(self._conventionMembers[each].remoteAddress,
                                  self._conventionMembers[each].registryValid)
        resp.setNotifyHandler(self._conventionNotificationHandler)
        super(ConventioneerAdmin, self)._updateStatusResponse(resp)


    def _activate(self):
        self.setupConvention()


    @property
    def _conventionLost(self):
        "True if this system was part of a convention but are no longer"
        return getattr(self, '_conventionLeaderIsGone', False)


    def setupConvention(self):
        if self.isShuttingDown(): return
        if not self._conventionAddress:
            gCA = getattr(self.transport, 'getConventionAddress', lambda c: None)
            self._conventionAddress = gCA(self.capabilities)
            if self._conventionAddress == self.myAddress:
                self._conventionAddress = None
        if self._conventionAddress:
            thesplog('Admin registering with Convention @ %s (%s)',
                     self._conventionAddress,
                     'first time' if getattr(self, '_conventionLeaderIsGone', True) else
                     're-registering',
                     level=logging.INFO, primary=True)
            self._hysteresisSender.sendWithHysteresis(
                TransmitIntent(self._conventionAddress,
                               ConventionRegister(self.myAddress,
                                                  self.capabilities,
                                                  getattr(self, '_conventionLeaderIsGone', True)),
                               onSuccess = self._setupConventionCBGood,
                               onError = self._setupConventionCBError))
            self._conventionRegistration = ExpiryTime(CONVENTION_REREGISTRATION_PERIOD)

    def _setupConventionCBGood(self, result, finishedIntent):
        self._sCBStats.inc('Admin Convention Registered')
        self._conventionLeaderIsGone = False
        if hasattr(self, '_conventionLeaderMissCount'):
            delattr(self, '_conventionLeaderMissCount')

    def _setupConventionCBError(self, result, finishedIntent):
        self._sCBStats.inc('Admin Convention Registration Failed')
        if hasattr(self, '_conventionLeaderMissCount'):
            self._conventionLeaderMissCount += 1
        else:
            self._conventionLeaderMissCount = 1
        if self._conventionLeaderMissCount < CONVENTION_REGISTRATION_MISS_MAX:
            thesplog('Admin cannot register with convention @ %s (miss %d): %s',
                     finishedIntent.targetAddr,
                     self._conventionLeaderMissCount,
                     result, level=logging.WARNING, primary=True)
        else:
            thesplog('Admin convention registration lost @ %s (miss %d): %s',
                     finishedIntent.targetAddr,
                     self._conventionLeaderMissCount,
                     result, level=logging.ERROR, primary=True)
            if hasattr(self, '_conventionLeaderIsGone'):
                self._conventionLeaderIsGone = True


    def isConventionLeader(self):
        return self._conventionAddress is None or self._conventionAddress == self.myAddress


    def h_ConventionRegister(self, envelope):
        if self.isShuttingDown(): return
        self._sCBStats.inc('Admin Handle Convention Registration')
        # Registrant may re-register if changing capabilities
        registrant = envelope.message.adminAddress
        thesplog('Got Convention registration from %s (%s) (new? %s)',
                 registrant, 'first time' if envelope.message.firstTime else 're-registering',
                 registrant.actorAddressString not in self._conventionMembers,
                 level=logging.INFO)
        if registrant == self.myAddress:
            # Either remote failed getting an external address and is
            # using 127.0.0.1 or else this is a malicious attempt to
            # make us talk to ourselves.  Ignore it.
            thesplog('Convention registration from %s is an invalid address; ignoring.',
                     registrant,
                     level=logging.WARNING)
            return True
        #erase our knowledge of actors associated with potential former instance of this system
        if envelope.message.firstTime:
            self._remoteSystemCleanup(registrant)
        if registrant == self._conventionAddress:
            self._conventionLeaderIsGone = False
        newReg = registrant.actorAddressString not in self._conventionMembers
        self._conventionMembers[registrant.actorAddressString] = \
                ConventionMemberData(registrant, envelope.message.capabilities)
        if self.isConventionLeader():

            if newReg:
                if self._conventionNotificationHandler:
                    self._send_intent(
                        TransmitIntent(self._conventionNotificationHandler,
                                       ActorSystemConventionUpdate(registrant,
                                                                   envelope.message.capabilities,
                                                                   True)))  # errors ignored

                # If we are the Convention Leader, this would be the point to
                # inform all other registrants of the new registrant.  At
                # present, there is no reciprocity here, so just update the
                # new registrant with the leader's info.

                self._send_intent(
                    TransmitIntent(registrant,
                                   ConventionRegister(self.myAddress,
                                                      self.capabilities,
                                                      # first time in, then must be first time out
                                                      envelope.message.firstTime)))

        return True


    def h_ConventionDeRegister(self, envelope):
        self._sCBStats.inc('Admin Handle Convention De-registration')
        remoteAdmin = envelope.message.adminAddress
        if remoteAdmin == self.myAddress:
            # Either remote failed getting an external address and is
            # using 127.0.0.1 or else this is a malicious attempt to
            # make us talk to ourselves.  Ignore it.
            thesplog('Convention deregistration from %s is an invalid address; ignoring.',
                     remoteAdmin,
                     level=logging.WARNING)
            return True
        self._remoteSystemCleanup(remoteAdmin)
        return True


    def h_SystemShutdown(self, envelope):
        if self._conventionAddress:
            thesplog('Admin de-registering with Convention @ %s',
                     str(self._conventionAddress), level=logging.INFO, primary=True)
            self._hysteresisSender.cancelSends(self._conventionAddress)
            self._send_intent(
                TransmitIntent(self._conventionAddress,
                               ConventionDeRegister(self.myAddress)))
        return super(ConventioneerAdmin, self).h_SystemShutdown(envelope)


    def _remoteSystemCleanup(self, registrant):
        """Called when a RemoteActorSystem has exited and all associated
           Actors should be marked as exited and the ActorSystem
           removed from Convention membership.
        """
        thesplog('Convention cleanup or deregistration for %s (new? %s)',
                 registrant,
                 registrant.actorAddressString not in self._conventionMembers,
                 level=logging.INFO)
        if registrant.actorAddressString in self._conventionMembers:
            cmr = self._conventionMembers[registrant.actorAddressString]

            # Send exited notification to conventionNotificationHandler (if any)
            if self.isConventionLeader() and self._conventionNotificationHandler:
                self._send_intent(
                    TransmitIntent(self._conventionNotificationHandler,
                                   ActorSystemConventionUpdate(cmr.remoteAddress,
                                                               cmr.remoteCapabilities,
                                                               False)))  # errors ignored

            # If the remote ActorSystem shutdown gracefully (i.e. sent
            # a Convention Deregistration) then it should not be
            # necessary to shutdown remote Actors (or notify of their
            # shutdown) because the remote ActorSystem should already
            # have caused this to occur.  However, it won't hurt, and
            # it's necessary if the remote ActorSystem did not exit
            # gracefully.

            for lpa, raa in cmr.hasRemoteActors:
                # ignore errors:
                self._send_intent(TransmitIntent(lpa, ChildActorExited(raa)))
                # n.b. at present, this means that the parent might
                # get duplicate notifications of ChildActorExited; it
                # is expected that Actors can handle this.

            # Remove remote system from conventionMembers
            del self._conventionMembers[registrant.actorAddressString]

        if registrant == self._conventionAddress:
            # Convention Leader has exited.  Do NOT set
            # conventionAddress to None.  It might speed up shutdown
            # of this ActorSystem because it won't try to de-register
            # from the convention leader, but if the convention leader
            # reappears there will be nothing to get this ActorSystem
            # re-registered with the convention.
            self._conventionLeaderIsGone = True

        self._hysteresisSender.cancelSends(registrant)


    def _checkConvention(self):
        if self.isConventionLeader():
            missing = []
            for each in self._conventionMembers:
                if self._conventionMembers[each].registryValid.expired():
                    missing.append(each)
            for each in missing:
                thesplog('%s missed %d checkins (%s); assuming it has died',
                         str(self._conventionMembers[each]),
                         CONVENTION_REGISTRATION_MISS_MAX,
                         str(self._conventionMembers[each].registryValid),
                         level=logging.WARNING, primary=True)
                self._remoteSystemCleanup(self._conventionMembers[each].remoteAddress)
            self._conventionRegistration = ExpiryTime(CONVENTION_REREGISTRATION_PERIOD)
        else:
            # Re-register with the Convention if it's time
            if self._conventionAddress and self._conventionRegistration.expired():
                self.setupConvention()


    def run(self):
        try:
            while not self.isShuttingDown():
                delay = min(self._conventionRegistration or \
                            ExpiryTime(CONVENTION_RESTART_PERIOD
                                       if self._conventionLost and not self.isConventionLeader() else
                                       CONVENTION_REREGISTRATION_PERIOD),
                            ExpiryTime(None) if self._hysteresisSender.delay.expired() else
                            self._hysteresisSender.delay
                )
                self.transport.run(self.handleIncoming, delay.remaining())
                self._checkConvention()
                self._hysteresisSender.checkSends()
        except Exception as ex:
            import traceback
            thesplog('ActorAdmin uncaught exception: %s', traceback.format_exc(),
                     level=logging.ERROR, exc_info=True)
        thesplog('Admin time to die', level=logging.DEBUG)


    # ---- Source Hash Transfers --------------------------------------------------

    def h_SourceHashTransferRequest(self, envelope):
        sourceHash = envelope.message.sourceHash
        self._send_intent(
            TransmitIntent(envelope.sender,
                           SourceHashTransferReply(sourceHash,
                                                   self._sources.get(sourceHash, None))))
        return True


    def h_SourceHashTransferReply(self, envelope):
        sourceHash = envelope.message.sourceHash
        pending = self._pendingSources[sourceHash]
        del self._pendingSources[sourceHash]
        if envelope.message.isValid():
            self._sources[sourceHash] = envelope.message.sourceData
            for each in pending:
                self.h_PendingActor(each)
        else:
            for each in pending:
                self._sendPendingActorResponse(each, None,
                                               errorCode = PendingActorResponse.ERROR_Invalid_SourceHash)
        return True


    def h_ValidateSource(self, envelope):
        if not envelope.message.sourceData and envelope.sender != self._conventionAddress:
            # Propagate source unload requests to all convention members
            for each in self._conventionMembers:
                self._hysteresisSender.sendWithHysteresis(
                    TransmitIntent(self._conventionMembers[each].remoteAddress,
                                   envelope.message))
        super(ConventioneerAdmin, self).h_ValidateSource(envelope)
        return False  # might have sent with hysteresis, so break out to local _run


    def _sentByRemoteAdmin(self, envelope):
        for each in self._conventionMembers:
            if envelope.sender == self._conventionMembers[each].remoteAddress:
                return True
        return False


    def _acceptsRemoteLoadedSourcesFrom(self, pendingActorEnvelope):
        allowed = self.capabilities.get('AllowRemoteActorSources', 'yes')
        return allowed.lower() == 'yes' or \
            (allowed == 'LeaderOnly' and
             pendingActorEnvelope.sender == self._conventionAddress)


    def h_PendingActor(self, envelope):
        sourceHash = envelope.message.sourceHash
        childRequirements = envelope.message.targetActorReq
        # If this request was forwarded by a remote Admin and the
        # sourceHash is not known locally, request it from the sending
        # remote Admin
        if sourceHash and \
           sourceHash not in self._sources and \
           self._sentByRemoteAdmin(envelope) and \
           self._acceptsRemoteLoadedSourcesFrom(envelope):
            requestedAlready = self._pendingSources.get(sourceHash, False)
            self._pendingSources.setdefault(sourceHash, []).append(envelope)
            if not requestedAlready:
                self._hysteresisSender.sendWithHysteresis(
                    TransmitIntent(envelope.sender,
                                   SourceHashTransferRequest(sourceHash)))
                return False  # sent with hysteresis, so break out to local _run
            return True
        # If the requested ActorClass is compatible with this
        # ActorSystem, attempt to start it, otherwise forward the
        # request to any known compatible ActorSystem.
        try:
            childClass = actualActorClass(envelope.message.actorClassName,
                                          partial(loadModuleFromHashSource,
                                                  sourceHash,
                                                  self._sources)
                                          if sourceHash else None)
            acceptsCaps = lambda caps: checkActorCapabilities(childClass, caps,
                                                              childRequirements)
            if not acceptsCaps(self.capabilities):
                if envelope.message.forActor is None:
                    # Request from external; use sender address
                    envelope.message.forActor = envelope.sender
                remoteCandidates = [
                    K
                    for K in self._conventionMembers
                    if not self._conventionMembers[K].registryValid.expired()
                    and self._conventionMembers[K].remoteAddress != envelope.sender # source Admin
                    and acceptsCaps(self._conventionMembers[K].remoteCapabilities)]
                if not remoteCandidates:
                    if self.isConventionLeader():
                        thesplog('No known ActorSystems can handle a %s for %s',
                                 childClass, envelope.message.forActor,
                                 level=logging.WARNING, primary=True)
                        self._sendPendingActorResponse(envelope, None,
                                                       errorCode = PendingActorResponse.ERROR_No_Compatible_ActorSystem)
                        return True
                    # Let the Convention Leader try to find an appropriate ActorSystem
                    bestC = self._conventionAddress
                else:
                    # distribute equally amongst candidates
                    C = [(self._conventionMembers[K].remoteAddress,
                          len(self._conventionMembers[K].hasRemoteActors))
                         for K in remoteCandidates]
                    bestC = foldl(lambda best,possible:
                                  best if best[1] <= possible[1] else possible,
                                  C)[0]
                self._send_intent(TransmitIntent(bestC, envelope.message))
                return True
        except InvalidActorSourceHash:
            self._sendPendingActorResponse(envelope, None,
                                           errorCode = PendingActorResponse.ERROR_Invalid_SourceHash)
            return True
        except InvalidActorSpecification:
            self._sendPendingActorResponse(envelope, None,
                                           errorCode = PendingActorResponse.ERROR_Invalid_ActorClass)
            return True
        except ImportError:
            self._sendPendingActorResponse(envelope, None,
                                           errorCode = PendingActorResponse.ERROR_Import)
            return True
        except AttributeError:
            # Usually when the module has no attribute FooActor
            self._sendPendingActorResponse(envelope, None,
                                           errorCode = PendingActorResponse.ERROR_Invalid_ActorClass)
            return True
        except Exception as ex:
            import traceback
            thesplog('Exception "%s" handling PendingActor: %s', ex, traceback.format_exc())
            self._sendPendingActorResponse(envelope, None,
                                           errorCode = PendingActorResponse.ERROR_Invalid_ActorClass)
            return True
        return super(ConventioneerAdmin, self).h_PendingActor(envelope)


    def h_NotifyOnSystemRegistration(self, envelope):
        if envelope.message.enableNotification:
            self._conventionNotificationHandler = envelope.sender
            # Now update the registrant on the current state of all convention members
            for member in self._conventionMembers:
                self._send_intent(
                    TransmitIntent(envelope.sender,
                                   ActorSystemConventionUpdate(member,
                                                               self._conventionMembers[member].remoteCapabilities,
                                                               True)))
        else:
            if envelope.sender == self._conventionNotificationHandler:
                self._conventionNotificationHandler = None
        return True


    def h_CapabilityUpdate(self, envelope):
        updateLocals = self._updSystemCapabilities(
            envelope.message.capabilityName,
            envelope.message.capabilityValue)
        self.setupConvention()
        if updateLocals: self._capUpdateLocalActors()
        return False  # might have sent with Hysteresis, so return to _run loop here
