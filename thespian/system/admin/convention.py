import logging
from thespian.actors import *
from thespian.system.utilis import (thesplog, checkActorCapabilities,
                                    foldl, join, fmap, AssocList,
                                    getenvdef, str_to_timedelta,
                                    actualActorClass)
from thespian.system.timing import ExpirationTimer, currentTime
from thespian.system.logdirector import LogAggregator
from thespian.system.admin.globalNames import GlobalNamesAdmin
from thespian.system.admin.adminCore import PendingSource
from thespian.system.transport import (TransmitIntent, ReceiveEnvelope,
                                       Thespian__Run_Terminated)
from thespian.system.messages.admin import PendingActorResponse
from thespian.system.messages.convention import *
from thespian.system.sourceLoader import loadModuleFromHashSource
from thespian.system.transport.hysteresis import HysteresisDelaySender
from functools import partial
from datetime import timedelta


CONVENTION_REREGISTRATION_PERIOD  = getenvdef('CONVENTION_REREGISTRATION_PERIOD',
                                              str_to_timedelta,
                                              timedelta(minutes=7, seconds=22))
CONVENTION_RESTART_PERIOD         = getenvdef('CONVENTION_RESTART_PERIOD',
                                              str_to_timedelta,
                                              timedelta(minutes=3, seconds=22))

# # of missing convention registrations before death declared
CONVENTION_REGISTRATION_MISS_MAX  = getenvdef('CONVENTION_REGISTRATION_MISS_MAX',
                                              int, 3)

CONVENTION_REINVITE_ADJUSTMENT    = 1.1  # multiply by remote checkin expected time for new invite timeout period


def convention_reinvite_adjustment(t):
    try:
        return t * CONVENTION_REINVITE_ADJUSTMENT
    except TypeError:
        # Python2 cannot multiply timedelta by a float, so take a longer route
        return t + (t / int(1 / (CONVENTION_REINVITE_ADJUSTMENT % 1)))


class PreRegistration(object):

    def __init__(self):
        self.pingValid   = ExpirationTimer(timedelta(seconds=0))
        self.pingPending = False

    def refresh(self):
        self.pingValid = ExpirationTimer(CONVENTION_REREGISTRATION_PERIOD)


class ConventionMemberData(object):
    def __init__(self, address, capabilities, preRegOnly=False):
        self.remoteAddress      = address
        self.remoteCapabilities = capabilities
        self.hasRemoteActors    = []  # (localParent, remoteActor) addresses created remotely

        # The preRegOnly field indicates that this information is only
        # from a pre-registration.
        self.preRegOnly = preRegOnly

        # preRegistered is not None if the ConventionRegister has the
        # preRegister flag set.  This indicates a call from
        # preRegisterRemoteSystem.  The pingValid is only used for
        # preRegistered systems and is used to determine how long an
        # active check of the preRegistered remote is valid.  If
        # pingValid is expired, the local attempts to send a
        # QueryExists message (which will retry) and a QueryAck will
        # reset pingValid to another CONVENTION_REGISTRATION_PERIOD.
        # The pingPending is true while the QueryExists is pending and
        # will suppress additional pingPending messages.  A success or
        # failure completion of a QueryExists message will reset
        # pingPending to false.  Note that pinging occurs continually
        # for a preRegistered remote, regardless of whether or not its
        # Convention membership is currently valid.
        self.preRegistered      = None   # or PreRegistration object

        self._reset_valid_timer()

    @property
    def permanentEntry(self):
        return bool(self.preRegOnly or self.preRegistered)

    def createdActor(self, localParentAddress, newActorAddress):
        entry = localParentAddress, newActorAddress
        if entry not in self.hasRemoteActors:
            self.hasRemoteActors.append(entry)

    def refresh(self, remoteCapabilities, preReg=False):
        self.remoteCapabilities = remoteCapabilities
        self._reset_valid_timer()
        if self.preRegistered:
            self.preRegistered.refresh()

    def _reset_valid_timer(self):
        # registryValid is a timer that is usually set to a multiple
        # of the convention re-registration period.  Each successful
        # convention re-registration resets the timer to the maximum
        # value (actually, it replaces this structure with a newly
        # generated structure).  If the timer expires, the remote is
        # declared as dead and the registration is removed (or
        # quiesced if it is a pre-registration).
        self.registryValid = ExpirationTimer(CONVENTION_REREGISTRATION_PERIOD *
                                             CONVENTION_REGISTRATION_MISS_MAX)

    def __str__(self):
        return 'ActorSystem @ %s%s, registry valid for %s with %s' % (
            str(self.remoteAddress),
            (' (prereg-only)' if self.preRegOnly else
             (' (prereg)' if self.preRegistered else '')),
            str(self.registryValid),
            str(self.remoteCapabilities))


class HysteresisCancel(object):
    def __init__(self, cancel_addr):
        self.cancel_addr = cancel_addr


class HysteresisSend(TransmitIntent): pass


class LostRemote(object):
    # tells transport to reset (close sockets, drop buffers, etc.)
    def __init__(self, lost_addr):
        self.lost_addr = lost_addr


class LocalConventionState(object):
    # The general process of leader management in an HA configuration
    # where there are multiple potential leaders is currently:
    #
    # 1. The self._conventionAddress is a list of potential leader
    #    addresses.  This is set from the capabilities (with an assist
    #    from the transport to map the addresses to a
    #    transport-specific address).  At present, it is assumed that
    #    all convention members are initialized with the same list,
    #    and in the same order (excluding invite-only members).
    #
    # 2. The current active leader is the "highest" leader: the one
    #    with the lowest index in the list (appears first) that is
    #    also currently active (as provided by the
    #    self._conventionMembers list that is already updated by
    #    active registrations and either active deregistrations or
    #    timeouts.  The self._conventionLeaderIdx is a helper to
    #    indicate the current active leader without searching the
    #    self._conventionMembers array.
    #
    # 3. The standard operational mode of a convention is that the
    #    leader is largely passive in terms of membership: members
    #    initially join and subsequently periodically check-in by
    #    sending a registration request (eliciting a corresponding
    #    response from the leader).  The leader removes them from the
    #    convention if they don't check-in within a specified period
    #    of time, but does not actively probe the member.  The
    #    principle behind this is that traffic should only be
    #    generated for active members and not inactive members.
    #
    # 4. With the addition of HA support, the member registration and
    #    check-in is always sent to *all* potential leaders,
    #    regardless of which is thought to be the current active
    #    leader by that member.  This includes all potential leaders,
    #    which send a registration to potential leaders higher than
    #    themselves.
    #
    # 5. When a potential leader receives a check-in registration, it
    #    will check to see if it believes itself to be the
    #    highest-priority active leader.  If so, it will respond and
    #    the remote will see that it is the current leader (including
    #    any other potential leaders, active or not).  Potential
    #    leaders that see a higher-priority leader will not respond to
    #    a check-in request, but will have updated their internal
    #    member information list.
    #
    # Based on the above, a leadership transition occurs naturally
    # (albeit slowly) through the passive combination of #2, #4, and
    # #5.  At present, there is no exchange of state information
    # between leaders, so any context maintained by one leader will be
    # lost in moving to a new leader [this is an area that should be
    # improved in future work]
    def __init__(self, myAddress, capabilities, sCBStats,
                 getConventionAddressFunc):
        self._myAddress = myAddress
        self._capabilities = capabilities
        self._sCBStats = sCBStats
        self._conventionMembers = AssocList() # key=Remote Admin Addr, value=ConventionMemberData
        self._conventionNotificationHandlers = []
        self._getConventionAddr = getConventionAddressFunc
        self._conventionLeaderIdx = 0
        self._conventionAddress = getConventionAddressFunc(capabilities)
        if not isinstance(self._conventionAddress, list):
            self._conventionAddress = [ self._conventionAddress ]
        self._conventionRegistration = ExpirationTimer(CONVENTION_REREGISTRATION_PERIOD)
        self._has_been_activated = False
        self._invited = False  # entered convention as a result of an explicit invite


    @property
    def myAddress(self):
        return self._myAddress

    @property
    def capabilities(self):
        return self._capabilities

    def updateStatusResponse(self, resp):
        resp.setConventionLeaderAddress(self.conventionLeaderAddr)
        resp.setConventionRegisterTime(self._conventionRegistration)
        for each in self._conventionMembers.values():
            resp.addConventioneer(each.remoteAddress, each.registryValid)
        resp.setNotifyHandlers(self._conventionNotificationHandlers)

    def active_in_convention(self):
        # If this is the convention leader, it is automatically
        # active, otherwise this convention member should have a
        # convention leader and that leader should have an active
        # entry in the _conventionMembers table (indicating it has
        # updated this system with its information)
        return bool(self.conventionLeaderAddr and
                    self._conventionMembers.find(self.conventionLeaderAddr))

    @property
    def conventionLeaderAddr(self):
        return self._conventionAddress[self._conventionLeaderIdx]

    def isConventionLeader(self):
        "Return true if this is the current leader of this convention"
        # This checks to see if the current system is the convention
        # leader.  This check is dynamic and may have the effect of
        # changing the determination of which is the actual convention
        # leader.
        if self._conventionAddress == [None]:
            return True
        for (idx,myLeader) in enumerate(self._conventionAddress):
            if myLeader == self.myAddress:
                # I am the highest active leader, therefore I am the
                # current actual leader.
                self._conventionLeaderIdx = idx
                return True
            if self._conventionMembers.find(myLeader):
                # A leader higher priority than myself exists (this is
                # actually the highest active due to the processing
                # order), so it's the leader, not me.
                self._conventionLeaderIdx = idx
                return False
        return False

    def capabilities_have_changed(self, new_capabilities):
        self._capabilities = new_capabilities
        return self.setup_convention()

    def setup_convention(self, activation=False):
        """Called to perform the initial registration with the convention
           leader (unless this *is* the leader) and also whenever
           connectivity to the convention leader is restored.
           Performs some administration and then attempts to register
           with the convention leader.
        """
        self._has_been_activated |= activation
        rmsgs = []
        # If not specified in capabilities, don't override any invites
        # that may have been received.
        self._conventionAddress = self._getConventionAddr(self.capabilities) or \
                                  self._conventionAddress
        if not isinstance(self._conventionAddress, list):
            self._conventionAddress = [ self._conventionAddress ]
        if self._conventionLeaderIdx >= len(self._conventionAddress):
            self._conventionLeaderIdx = 0
        leader_is_gone = (self._conventionMembers.find(self.conventionLeaderAddr) is None) \
                         if self.conventionLeaderAddr else True
        # Register with all other leaders to notify them that this potential leader is online
        if self._conventionAddress and \
           self._conventionAddress[0] != None:
            for possibleLeader in self._conventionAddress:
                if possibleLeader == self.myAddress:
                    # Don't register with myself
                    continue
                re_registering = not leader_is_gone and \
                    (possibleLeader == self.conventionLeaderAddr)
                thesplog('Admin registering with Convention @ %s (%s)',
                         possibleLeader,
                         'first time' if not re_registering else 're-registering',
                         level=logging.INFO, primary=True)
                rmsgs.append(
                    HysteresisSend(possibleLeader,
                                   ConventionRegister(self.myAddress,
                                                      self.capabilities,
                                                      not re_registering),
                                   onSuccess = self._setupConventionCBGood,
                                   onError = self._setupConventionCBError))
        self._conventionRegistration = ExpirationTimer(CONVENTION_REREGISTRATION_PERIOD)
        return rmsgs

    def _setupConventionCBGood(self, result, finishedIntent):
        self._sCBStats.inc('Admin Convention Registered')
        if hasattr(self, '_conventionLeaderMissCount'):
            delattr(self, '_conventionLeaderMissCount')

    def _setupConventionCBError(self, result, finishedIntent):
        self._sCBStats.inc('Admin Convention Registration Failed')
        if hasattr(self, '_conventionLeaderMissCount'):
            self._conventionLeaderMissCount += 1
        else:
            self._conventionLeaderMissCount = 1
        thesplog('Admin cannot register with convention @ %s (miss %d): %s',
                 finishedIntent.targetAddr,
                 self._conventionLeaderMissCount,
                 result, level=logging.WARNING, primary=True)

    def got_convention_invite(self, sender):
        self._conventionAddress = [sender]
        self._conventionLeaderIdx = 0
        self._invited = True
        return self.setup_convention()

    def got_convention_register(self, regmsg):
        # Called when remote convention member has sent a
        # ConventionRegister message.  This is first called the leader
        # when the member registers with the leader, and then on the
        # member when the leader responds with same.  Thus the current
        # node could be a member, a potential leader, the current
        # leader, or a potential leader with higher potential than the
        # current leader and which should become the new leader.
        self._sCBStats.inc('Admin Handle Convention Registration')
        if self._invited and not self.conventionLeaderAddr:
            # Lost connection to an invitation-only convention.
            # Cannot join again until another invitation is received.
            return []
        # Remote member may re-register if changing capabilities
        rmsgs = []
        registrant = regmsg.adminAddress
        prereg = getattr(regmsg, 'preRegister', False)  # getattr used; see definition
        existing = self._conventionMembers.find(registrant)
        thesplog('Got Convention %sregistration from %s (%s) (new? %s)',
                 'pre-' if prereg else '',
                 registrant,
                 'first time' if regmsg.firstTime else 're-registering',
                 not existing,
                 level=logging.DEBUG)
        if registrant == self.myAddress:
            # Either remote failed getting an external address and is
            # using 127.0.0.1 or else this is a malicious attempt to
            # make us talk to ourselves.  Ignore it.
            thesplog('Convention registration from %s is an invalid address; ignoring.',
                     registrant,
                     level=logging.WARNING)
            return rmsgs

        existingPreReg = (
            # existing.preRegOnly
            # or existing.preRegistered
            existing.permanentEntry
        ) if existing else False
        notify = (not existing or existing.preRegOnly) and not prereg

        if regmsg.firstTime or not existing:
            if existing:
                existing = None
                notify = not prereg
                rmsgs.extend(self._remote_system_cleanup(registrant))
            newmember = ConventionMemberData(registrant,
                                             regmsg.capabilities,
                                             prereg)
            if prereg or existingPreReg:
                newmember.preRegistered = PreRegistration()
            self._conventionMembers.add(registrant, newmember)
        else:
            existing.refresh(regmsg.capabilities, prereg or existingPreReg)
            if not prereg:
                existing.preRegOnly = False

        if not self.isConventionLeader():
            self._conventionRegistration = ExpirationTimer(CONVENTION_REREGISTRATION_PERIOD)
            rmsgs.append(LogAggregator(self.conventionLeaderAddr))

        # Convention Members normally periodically initiate a
        # membership message, to which the leader confirms by
        # responding.
        #if self.isConventionLeader() or prereg or regmsg.firstTime:
        if prereg:
            # If this was a pre-registration, that identifies this
            # system as the "leader" for that remote.  Also, if the
            # remote sent this because it was a pre-registration
            # leader, it doesn't yet have all the member information
            # so the member should respond.
            rmsgs.append(HysteresisCancel(registrant))
            rmsgs.append(TransmitIntent(registrant, ConventionInvite()))
        elif (self.isConventionLeader() or prereg or regmsg.firstTime or \
           (existing and existing.permanentEntry)):
            # If we are the Convention Leader, this would be the point to
            # inform all other registrants of the new registrant.  At
            # present, there is no reciprocity here, so just update the
            # new registrant with the leader's info.
            rmsgs.append(
                TransmitIntent(registrant,
                               ConventionRegister(self.myAddress,
                                                  self.capabilities)))

        if notify:
            rmsgs.extend(self._notifications_of(
                ActorSystemConventionUpdate(registrant,
                                            regmsg.capabilities,
                                            True)))
        return rmsgs

    def _notifications_of(self, msg):
        return [TransmitIntent(H, msg) for H in self._conventionNotificationHandlers]

    def add_notification_handler(self, addr):
        if addr not in self._conventionNotificationHandlers:
            self._conventionNotificationHandlers.append(addr)
            # Now update the registrant on the current state of all convention members
            return [TransmitIntent(addr,
                                   ActorSystemConventionUpdate(M.remoteAddress,
                                                               M.remoteCapabilities,
                                                               True))
                    for M in self._conventionMembers.values()
                    if not M.preRegOnly]
        return []

    def remove_notification_handler(self, addr):
        self._conventionNotificationHandlers = [
            H for H in self._conventionNotificationHandlers
            if H != addr]

    def got_convention_deregister(self, deregmsg):
        self._sCBStats.inc('Admin Handle Convention De-registration')
        remoteAdmin = deregmsg.adminAddress
        if remoteAdmin == self.myAddress:
            # Either remote failed getting an external address and is
            # using 127.0.0.1 or else this is a malicious attempt to
            # make us talk to ourselves.  Ignore it.
            thesplog('Convention deregistration from %s is an invalid address; ignoring.',
                     remoteAdmin,
                     level=logging.WARNING)
        rmsgs = []
        if getattr(deregmsg, 'preRegistered', False): # see definition for getattr use
            existing = self._conventionMembers.find(remoteAdmin)
            if existing:
                existing.preRegistered = None
                rmsgs.append(TransmitIntent(remoteAdmin, ConventionDeRegister(self.myAddress)))
        return rmsgs + self._remote_system_cleanup(remoteAdmin)

    def got_system_shutdown(self):
        return self.exit_convention()

    def exit_convention(self):
        self.invited = False
        gen_ops = lambda addr: [HysteresisCancel(addr),
                                TransmitIntent(addr,
                                               ConventionDeRegister(self.myAddress)),
        ]
        terminate = lambda a: [ self._remote_system_cleanup(a), gen_ops(a) ][-1]
        if self.conventionLeaderAddr and \
           self.conventionLeaderAddr != self.myAddress:
            thesplog('Admin de-registering with Convention @ %s',
                     str(self.conventionLeaderAddr),
                     level=logging.INFO, primary=True)
            # Cache convention leader address because it might get reset by terminate()
            claddr = self.conventionLeaderAddr
            terminate(self.conventionLeaderAddr)
            return gen_ops(claddr)
        return join(fmap(terminate,
                         [M.remoteAddress
                          for M in self._conventionMembers.values()
                          if M.remoteAddress != self.myAddress]))

    def check_convention(self):
        ct = currentTime()
        rmsgs = []
        if self._has_been_activated:
            rmsgs = foldl(lambda x, y: x + y,
                          [self._check_preregistered_ping(ct, member)
                           for member in self._conventionMembers.values()],
                          self._convention_leader_checks(ct)
                          if self.isConventionLeader() or
                          not self.conventionLeaderAddr else
                          self._convention_member_checks(ct))
        if self._conventionRegistration.view(ct).expired():
            self._conventionRegistration = ExpirationTimer(CONVENTION_REREGISTRATION_PERIOD)
        return rmsgs

    def _convention_leader_checks(self, ct):
        return foldl(lambda x, y: x + y,
                     [self._missed_checkin_remote_cleanup(R)
                      for R in [ member
                                 for member in self._conventionMembers.values()
                                 if member.registryValid.view(ct).expired() ]],
                     [])

    def _missed_checkin_remote_cleanup(self, remote_member):
        thesplog('%s missed %d checkins (%s); assuming it has died',
                 str(remote_member),
                 CONVENTION_REGISTRATION_MISS_MAX,
                 str(remote_member.registryValid),
                 level=logging.WARNING, primary=True)
        return self._remote_system_cleanup(remote_member.remoteAddress)


    def _convention_member_checks(self, ct):
        rmsgs = []
        # Re-register with the Convention if it's time
        if self.conventionLeaderAddr and \
           self._conventionRegistration.view(ct).expired():
            if getattr(self, '_conventionLeaderMissCount', 0) >= \
               CONVENTION_REGISTRATION_MISS_MAX:
                thesplog('Admin convention registration lost @ %s (miss %d)',
                         self.conventionLeaderAddr,
                         self._conventionLeaderMissCount,
                         level=logging.WARNING, primary=True)
                rmsgs.extend(self._remote_system_cleanup(self.conventionLeaderAddr))
                self._conventionLeaderMissCount = 0
            else:
                rmsgs.extend(self.setup_convention())
        return rmsgs

    def _check_preregistered_ping(self, ct, member):
        if member.preRegistered and \
           member.preRegistered.pingValid.view(ct).expired() and \
           not member.preRegistered.pingPending:
            member.preRegistered.pingPending = True
            # If remote misses a checkin, re-extend the
            # invitation.  This also helps re-initiate a socket
            # connection if a TxOnly socket has been lost.
            member.preRegistered.pingValid = ExpirationTimer(
                convention_reinvite_adjustment(
                    CONVENTION_RESTART_PERIOD
                    if member.registryValid.view(ct).expired()
                    else CONVENTION_REREGISTRATION_PERIOD))
            return [HysteresisSend(member.remoteAddress,
                                   ConventionInvite(),
                                   onSuccess = self._preRegQueryNotPending,
                                   onError = self._preRegQueryNotPending)]
        return []

    def _preRegQueryNotPending(self, result, finishedIntent):
        remoteAddr = finishedIntent.targetAddr
        member = self._conventionMembers.find(remoteAddr)
        if member and member.preRegistered:
            member.preRegistered.pingPending = False

    def _remote_system_cleanup(self, registrant):
        """Called when a RemoteActorSystem has exited and all associated
           Actors should be marked as exited and the ActorSystem
           removed from Convention membership.  This is also called on
           a First Time connection from the remote to discard any
           previous connection information.

        """
        thesplog('Convention cleanup or deregistration for %s (known? %s)',
                 registrant,
                 bool(self._conventionMembers.find(registrant)),
                 level=logging.INFO)
        rmsgs = [LostRemote(registrant)]
        cmr = self._conventionMembers.find(registrant)
        if not cmr or cmr.preRegOnly:
            return []

        # Send exited notification to conventionNotificationHandler (if any)
        for each in self._conventionNotificationHandlers:
            rmsgs.append(
                TransmitIntent(each,
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
            rmsgs.append(TransmitIntent(lpa, ChildActorExited(raa)))
            # n.b. at present, this means that the parent might
            # get duplicate notifications of ChildActorExited; it
            # is expected that Actors can handle this.

        # Remove remote system from conventionMembers
        if not cmr.preRegistered:
            cla = self.conventionLeaderAddr
            self._conventionMembers.rmv(registrant)
            if registrant == cla:
                if self._invited:
                    # Don't clear invited: once invited, that
                    # perpetually indicates this should be only a
                    # member and never a leader.
                    self._conventionAddress = [None]
                else:
                    rmsgs.extend(self.setup_convention())
        else:
            # This conventionMember needs to stay because the
            # current system needs to continue issuing
            # registration pings.  By setting the registryValid
            # expiration to forever, this member won't re-time-out
            # and will therefore be otherwise ignored... until it
            # registers again at which point the membership will
            # be updated with new settings.
            cmr.registryValid = ExpirationTimer(None)
            cmr.preRegOnly = True

        return rmsgs + [HysteresisCancel(registrant)]

    def sentByRemoteAdmin(self, envelope):
        for each in self._conventionMembers.values():
            if envelope.sender == each.remoteAddress:
                return True
        return False

    def convention_inattention_delay(self, current_time):
        return (self._conventionRegistration or
                ExpirationTimer(CONVENTION_REREGISTRATION_PERIOD
                                if self.active_in_convention() or
                                self.isConventionLeader() else
                                CONVENTION_RESTART_PERIOD)).view(current_time)

    def forward_pending_to_remote_system(self, childClass, envelope, sourceHash, acceptsCaps):
        alreadyTried = getattr(envelope.message, 'alreadyTried', [])
        ct = currentTime()
        if self.myAddress not in alreadyTried:
            # Don't send request back to this actor system: it cannot
            # handle it
            alreadyTried.append(self.myAddress)

        remoteCandidates = [
            K
            for K in self._conventionMembers.values()
            if not K.registryValid.view(ct).expired()
            and K.remoteAddress != envelope.sender # source Admin
            and K.remoteAddress not in alreadyTried
            and acceptsCaps(K.remoteCapabilities)]

        if not remoteCandidates:
            if self.isConventionLeader() or not self.conventionLeaderAddr:
                raise NoCompatibleSystemForActor(
                    childClass,
                    'No known ActorSystems can handle a %s for %s',
                    childClass, envelope.message.forActor)
            # Let the Convention Leader try to find an appropriate ActorSystem
            bestC = self.conventionLeaderAddr
        else:
            # distribute equally amongst candidates
            C = [(K.remoteAddress, len(K.hasRemoteActors))
                 for K in remoteCandidates]
            bestC = foldl(lambda best,possible:
                          best if best[1] <= possible[1] else possible,
                          C)[0]
            thesplog('Requesting creation of %s%s on remote admin %s',
                     envelope.message.actorClassName,
                     ' (%s)'%sourceHash if sourceHash else '',
                     bestC)
        if bestC in alreadyTried:
            return []  # Have to give up, no-one can handle this

        # Don't send request to this remote again, it has already
        # been tried.  This would also be indicated by that system
        # performing the add of self.myAddress as below, but if
        # there is disagreement between the local and remote
        # addresses, this addition will prevent continual
        # bounceback.
        alreadyTried.append(bestC)
        envelope.message.alreadyTried = alreadyTried
        return [TransmitIntent(bestC, envelope.message)]


    def send_to_all_members(self, message, exception_list=None):
        return [HysteresisSend(M.remoteAddress, message)
                for M in self._conventionMembers.values()
                if M.remoteAddress not in (exception_list or [])]



class ConventioneerAdmin(GlobalNamesAdmin):
    """Extends the AdminCore+GlobalNamesAdmin with ActorSystem Convention
       functionality to support multi-host configurations.
    """
    def __init__(self, *args, **kw):
        super(ConventioneerAdmin, self).__init__(*args, **kw)
        self._cstate = LocalConventionState(
            self.myAddress,
            self.capabilities,
            self._sCBStats,
            getattr(self.transport, 'getConventionAddress', lambda c: None))
        self._hysteresisSender = HysteresisDelaySender(self._send_intent)

    def _updateStatusResponse(self, resp):
        self._cstate.updateStatusResponse(resp)
        super(ConventioneerAdmin, self)._updateStatusResponse(resp)


    def _activate(self):
        # Called internally when this ActorSystem has been initialized
        # and should be activated for operations.
        super(ConventioneerAdmin, self)._activate()
        if self.isShuttingDown(): return
        self._performIO(self._cstate.setup_convention(True))

    def h_ConventionInvite(self, envelope):
        if self.isShuttingDown(): return
        return self._performIO(self._cstate.got_convention_invite(envelope.sender))

    def h_ConventionRegister(self, envelope):
        if self.isShuttingDown(): return
        return self._performIO(self._cstate.got_convention_register(envelope.message))


    def h_ConventionDeRegister(self, envelope):
        return self._performIO(self._cstate.got_convention_deregister(envelope.message))

    def h_SystemShutdown(self, envelope):
        self._performIO(self._cstate.got_system_shutdown())
        return super(ConventioneerAdmin, self).h_SystemShutdown(envelope)
        return True

    def _performIO(self, iolist):
        rval = True
        for msg in iolist:
            if isinstance(msg, HysteresisCancel):
                self._hysteresisSender.cancelSends(msg.cancel_addr)
                rval = False
            elif isinstance(msg, HysteresisSend):
                #self._send_intent(msg)
                self._hysteresisSender.sendWithHysteresis(msg)
                rval = False
            elif isinstance(msg, LogAggregator):
                if getattr(self, 'asLogger', None):
                    thesplog('Setting log aggregator of %s to %s', self.asLogger, msg.aggregatorAddress)
                    self._send_intent(TransmitIntent(self.asLogger, msg))
            elif isinstance(msg, LostRemote):
                if hasattr(self.transport, 'lostRemote'):
                    self.transport.lostRemote(msg.lost_addr)
            else:
                self._send_intent(msg)
        return rval

    def run(self):
        # Main loop for convention management.  Wraps the lower-level
        # transport with a stop at the next needed convention
        # registration period to re-register.
        transport_continue = True
        try:
            while not getattr(self, 'shutdown_completed', False) and \
                  not isinstance(transport_continue, Thespian__Run_Terminated):
                ct = currentTime()
                delay = min(self._cstate.convention_inattention_delay(ct),
                            ExpirationTimer(None).view(ct) if self._hysteresisSender.delay.expired() else
                            self._hysteresisSender.delay
                )
                # n.b. delay does not account for soon-to-expire
                # pingValids, but since delay will not be longer than
                # a CONVENTION_REREGISTRATION_PERIOD, the worst case
                # is a doubling of a pingValid period (which should be fine).
                transport_continue = self.transport.run(self.handleIncoming,
                                                        delay.remaining())

                # Check Convention status based on the elapsed time
                self._performIO(self._cstate.check_convention())

                self._hysteresisSender.checkSends()
                self._remove_expired_sources()

        except Exception as ex:
            import traceback
            thesplog('ActorAdmin uncaught exception: %s', traceback.format_exc(),
                     level=logging.ERROR, exc_info=True)
        thesplog('Admin time to die', level=logging.DEBUG)


    # ---- Source Hash Transfers --------------------------------------------------

    def h_SourceHashTransferRequest(self, envelope):
        sourceHash = envelope.message.sourceHash
        src = self._sources.get(sourceHash, None)
        if not src or not src.source_valid:
            self._send_intent(
                TransmitIntent(envelope.sender,
                               SourceHashTransferReply(sourceHash)))
        else:
            # Older requests did not have the prefer_original field;
            # maintain backward compatibility
            orig = getattr(envelope.message, 'prefer_original', False)
            self._send_intent(
                TransmitIntent(
                    envelope.sender,
                    SourceHashTransferReply(
                        sourceHash,
                        src.orig_data if orig else src.zipsrc,
                        src.srcInfo,
                        original_form = orig)))
        return True


    def h_SourceHashTransferReply(self, envelope):
        sourceHash = envelope.message.sourceHash
        if sourceHash not in self._sources:
            return True
        if envelope.message.isValid():
            # nb.. original_form added; use getattr for backward compatibility
            if getattr(envelope.message, 'original_form', False):
                if self._sourceAuthority:
                    self._send_intent(
                        TransmitIntent(
                            self._sourceAuthority,
                            ValidateSource(sourceHash,
                                           envelope.message.sourceData,
                                           getattr(envelope.message,
                                                   'sourceInfo', None))))
                    return True
            else:
                self._loadValidatedActorSource(sourceHash,
                                               envelope.message.sourceData,
                                               # sourceInfo added; backward compat.
                                               getattr(envelope.message,
                                                       'sourceInfo', None))
                return True

        self._cancel_pending_actors(self._sources[sourceHash].pending_actors)
        del self._sources[sourceHash]
        return True


    def h_ValidateSource(self, envelope):
        rval = True
        if not envelope.message.sourceData and \
           envelope.sender != self._cstate.conventionLeaderAddr:
            # Propagate source unload requests to all convention members
            rval = self._performIO(
                self._cstate.send_to_all_members(
                    envelope.message,
                    # Do not propagate if this is where the
                    # notification came from; prevents indefinite
                    # bouncing of this message as long as the
                    # convention structure is a DAG.
                    [envelope.sender]))
        super(ConventioneerAdmin, self).h_ValidateSource(envelope)
        return rval


    def _acceptsRemoteLoadedSourcesFrom(self, pendingActorEnvelope):
        allowed = self.capabilities.get('AllowRemoteActorSources', 'yes')
        return allowed.lower() == 'yes' or \
            (allowed == 'LeaderOnly' and
             pendingActorEnvelope.sender == self._cstate.conventionLeaderAddr)


    # ---- Remote Actor interactions ----------------------------------------------


    def _not_compatible(self, createActorEnvelope):
        # Called when the current Actor System is not compatible with
        # the Actor's actorSystemCapabilityCheck.  Forward this
        # createActor request to another system that it's compatible
        # with.
        sourceHash = createActorEnvelope.message.sourceHash
        childRequirements = createActorEnvelope.message.targetActorReq
        childCName = createActorEnvelope.message.actorClassName
        childClass = actualActorClass(childCName,
                                      partial(loadModuleFromHashSource,
                                              sourceHash,
                                              self._sources)
                                      if sourceHash else None)
        acceptsCaps = lambda caps: checkActorCapabilities(childClass, caps,
                                                          childRequirements)
        if createActorEnvelope.message.forActor is None:
            # Request from external; use sender address
            createActorEnvelope.message.forActor = createActorEnvelope.sender
        iolist = self._cstate.forward_pending_to_remote_system(
            childClass, createActorEnvelope, sourceHash, acceptsCaps)
        if iolist:
            for each in iolist:
                # Expected to be only one; if the transmit fails,
                # route it back here so that the next possible
                # remote can be tried.
                each.addCallback(onFailure=self._pending_send_failed)
                each.orig_create_envelope = createActorEnvelope
            return self._performIO(iolist)
        self._sendPendingActorResponse(
            createActorEnvelope, None,
            errorCode = PendingActorResponse.ERROR_No_Compatible_ActorSystem,
            errorStr="")
        # self._retryPendingChildOperations(childInstance, None)
        return True


    def _get_missing_source_for_hash(self, sourceHash, createActorEnvelope):
        # If this request was forwarded by a remote Admin and the
        # sourceHash is not known locally, request it from the sending
        # remote Admin
        if self._cstate.sentByRemoteAdmin(createActorEnvelope) and \
           self._acceptsRemoteLoadedSourcesFrom(createActorEnvelope):
            self._sources[sourceHash] = PendingSource(sourceHash, None)
            self._sources[sourceHash].pending_actors.append(createActorEnvelope)
            self._hysteresisSender.sendWithHysteresis(
                TransmitIntent(
                    createActorEnvelope.sender,
                    SourceHashTransferRequest(sourceHash,
                                              bool(self._sourceAuthority))))
            # sent with hysteresis, so break out to local _run
            return False

        # No remote Admin to send the source, so fail as normal.
        return super(ConventioneerAdmin, self)._get_missing_source_for_hash(
            sourceHash,
            createActorEnvelope)


    def _pending_send_failed(self, result, intent):
        self._not_compatible(intent.orig_create_envelope)


    def h_NotifyOnSystemRegistration(self, envelope):
        if envelope.message.enableNotification:
            return self._performIO(
                self._cstate.add_notification_handler(envelope.sender))
        self._cstate.remove_notification_handler(envelope.sender)
        return True


    def h_PoisonMessage(self, envelope):
        self._cstate.remove_notification_handler(envelope.sender)


    def _handleChildExited(self, childAddr):
        self._cstate.remove_notification_handler(childAddr)
        return super(ConventioneerAdmin, self)._handleChildExited(childAddr)


    def h_CapabilityUpdate(self, envelope):
        msg = envelope.message
        updateLocals = self._updSystemCapabilities(msg.capabilityName,
                                                   msg.capabilityValue)
        rval = True
        if not self.isShuttingDown():
            rval = self._performIO(
                self._cstate.capabilities_have_changed(self.capabilities))
        if updateLocals:
            self._capUpdateLocalActors()
        return rval
