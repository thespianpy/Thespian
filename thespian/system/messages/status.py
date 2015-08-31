"Status messages for querying Actors or ActorSystems"

from thespian.actors import ActorSystemMessage


# ----------------------------------------------------------------------
# ActorSystem diagnostics
#
# An ActorSystem should support the following messages which allow
# diagnostic queries and monitoring.

class Thespian_StatusReq(ActorSystemMessage):
    "Sent to an ActorSystem or an Actor to request a status response"
    pass


class _Common_StatusResp(ActorSystemMessage):
    def __init__(self):
        self.pendingMessages  = []  # array of (fromActor, toActor, msgstr) tuples
        self.pendingWakeups   = {}  # key = wakeup time, value = array of WakeupMessages
        self.receivedMessages = []  # array of (fromActor, toActor, msgstr) tuples
        self.childActors      = []  # array of addresses
        self.governer         = None
        self._numSent         = 0
        self._numSendFailures = 0
        self._numReceived     = 0
        self.miscKeyVals      = {}
        self._pendingAddrCnts = {}  # key = local address, value = # of TX pending resolution of that address
    def addChild(self, childAddress): self.childActors.append(childAddress)
    def addPendingMessage(self, fromActor, toActor, msgstr):
        self.pendingMessages.append( (fromActor, toActor, str(msgstr)) )
    def addReceivedMessage(self, fromActor, toActor, msgstr):
        self.receivedMessages.append( (fromActor, toActor, str(msgstr)) )
    def addWakeups(self, wakeupdict):
        self.pendingWakeups.update(wakeupdict)
    def addSent(self, count): self._numSent += count
    def addSendFailures(self, count): self._numSendFailures += count
    def addReceived(self, count): self._numReceived += count
    def addKeyVal(self, key, value): self.miscKeyVals[key] = value
    def addTXPendingAddressCount(self, address, count):
        self._pendingAddrCnts[address] = count + self._pendingAddrCnts.get(address, 0)


class Thespian_SystemStatus(_Common_StatusResp):
    "ActorSystem response to a Thespian_StatusReq"
    def __init__(self, address, conventionLeader=None, capabilities=None, inShutdown=False):
        super(Thespian_SystemStatus, self).__init__()
        self.adminAddress           = address
        self.conventionLeader       = conventionLeader
        self.capabilities           = {} if capabilities is None else capabilities
        self.conventionRegisterTime = None
        self.conventionAttendees    = []  # array of (Admin actorAddress, last checkin or remaining time)
        self.deadLetterHandler      = None
        self.deadLetterAddresses    = []
        self.notifyAddress          = None
        self.globalActors           = {}
        self.inShutdown             = inShutdown
        self.sourceAuthority        = None
        self.loadedSources          = []
    def addDeadLetter(self, deadAddress): self.deadLetterAddresses.append(deadAddress)
    def setConventionLeaderAddress(self, addr): self.conventionLeader = addr
    def addConventioneer(self, memberAddress, validTime):
        self.conventionAttendees.append( (memberAddress, validTime) )
    def setConventionRegisterTime(self, time): self.conventionRegisterTime = time
    def addGlobalActor(self, name, address): self.globalActors[name] = address
    def setDeadLetterHandler(self, address): self.deadLetterHandler = address
    def setNotifyHandler(self, address): self.notifyAddress = address
    def setLoadedSources(self, sourceHashes): self.loadedSources = sourceHashes


# Likewise the management of each Actor should support the following diagnostics

class Thespian_ActorStatus(_Common_StatusResp):
    def __init__(self, address, actorClass, adminAddress, parentAddress=None, sourceHash=None):
        super(Thespian_ActorStatus, self).__init__()
        self.actorAddress    = address
        self.actorClass      = str(actorClass)
        self.adminAddress    = adminAddress
        self.parentAddress   = parentAddress
        self.sourceHash      = sourceHash



def _common_formatStatus(tofd, response, childActorTag, showAddress=str):
    tofd.write('  |%s Actors [%d]:\n'%(childActorTag, len(response.childActors)))
    for A in response.childActors:
        tofd.write('    @ %s\n'%(showAddress(A)))
    if response.governer:
        tofd.write('  |Rate Governer: %s\n'%(str(response.governer)))
    tofd.write('  |Pending Messages [%d]:\n'%len(response.pendingMessages))
    for F,T,M in response.pendingMessages:
        tofd.write('    %s --> %s:  %s\n'%(showAddress(F), showAddress(T), M))
    tofd.write('  |Received Messages [%d]:\n'%len(response.receivedMessages))
    for F,T,M in response.receivedMessages:
        tofd.write('    %s <-- %s:  %s\n'%(showAddress(T), showAddress(F), M))
    tofd.write('  |Pending Wakeups [%d]:\n'%len(response.pendingWakeups))
    import datetime
    for W in response.pendingWakeups:
        tofd.write('    %s%s\n'%(str(W),
                                 (' (in %s)'%(W - datetime.datetime.now()) if isinstance(W, datetime.datetime)
                                  else '')))
    tofd.write('  |Pending Address Resolution [%d]:\n'%(len(response._pendingAddrCnts)))
    for A in response._pendingAddrCnts:
        tofd.write('    %s: %s\n'%(A, response._pendingAddrCnts[A]))
    if response.miscKeyVals:
        miscKeys = list(response.miscKeyVals.keys())
        miscKeys.sort()
        maxlen = max([len(K) for K in miscKeys])
        for K in miscKeys:
            tofd.write('  |> %%%ds: %%%%s\n' % maxlen % K % str(response.miscKeyVals[K]))


def formatStatus(response, showAddress=str, tofd=None):
    if tofd is None:
        import sys
        tofd = sys.stdout
    if isinstance(response, Thespian_SystemStatus):
        tofd.write('Status of ActorSystem @ %s:%s\n'%(showAddress(response.adminAddress),
                                                      '  [IN SHUTDOWN]' if response.inShutdown else ''))
        tofd.write('  |Capabilities[%d]:\n'%len(response.capabilities))
        for k in response.capabilities:
            tofd.write('    %29s: %s\n'%(k, response.capabilities[k]))
        if response.conventionLeader:
            tofd.write('  |Convention Leader: %s\n'%(showAddress(response.conventionLeader)))
            if response.conventionRegisterTime:
                tofd.write('  Registration valid %s\n'%(str(response.conventionRegisterTime)))
        elif response.conventionAttendees:
            tofd.write('  Appears to be the Convention Leader\n')
        if response.notifyAddress:
            tofd.write('  |Convention Notifications: %s\n'%(showAddress(response.notifyAddress)))
        tofd.write('  |Convention Attendees [%d]:\n'%len(response.conventionAttendees))
        for addr,validtime in response.conventionAttendees:
            tofd.write('    @ %s: %s\n'%(showAddress(addr), str(validtime)))
        _common_formatStatus(tofd, response, 'Primary', showAddress)
        if response.deadLetterHandler:
            tofd.write('  |DeadLetter Handler: %s\n'%showAddress(response.deadLetterHandler))
        tofd.write('  |DeadLetter Addresses [%d]:\n'%len(response.deadLetterAddresses))
        for A in response.deadLetterAddresses:
            tofd.write('    %s\n'%(showAddress(A)))
        tofd.write('  |Source Authority: %s\n'%(showAddress(response.sourceAuthority)))
        tofd.write('  |Loaded Sources [%d]:\n'%len(response.loadedSources))
        for S in response.loadedSources:
            tofd.write('    %s\n'%(str(S)))
        tofd.write('  |Global Actors [%d]:\n'%len(response.globalActors))
        for N in response.globalActors:
            tofd.write('    %s: %s\n'%(N, showAddress(response.globalActors[N])))
    elif isinstance(response, Thespian_ActorStatus):
        tofd.write('Status of %s Actor @ %s:\n'%(response.actorClass,
                                                 showAddress(response.actorAddress)))
        if response.sourceHash:
            tofd.write('  |Source Hash: %s\n'%(response.sourceHash))
        tofd.write('  |Administrator: %s\n'%(showAddress(response.adminAddress)))
        tofd.write('  |Parent  Actor: %s\n'%(showAddress(response.parentAddress)))
        _common_formatStatus(tofd, response, 'Child', showAddress)
    else:
        tofd.write('Status Query Response: %s\n'%(str(response)))
