"Messages used to interact with the ActorSystem Admin"

from thespian.actors import ActorSystemMessage


class QueryExists(ActorSystemMessage):
    """Sent by system Base to the Admin address to determine if there is
       an Admin running at that address."""
    pass


class QueryAck(ActorSystemMessage):
    def __init__(self, systemName, systemVersion, inShutdown):
        self.systemName    = systemName
        self.systemVersion = systemVersion
        self.inShutdown    = inShutdown


class SystemShutdown(ActorSystemMessage): pass   # Message sent to Admin when shutting down the entire ActorSystem
class SystemShutdownCompleted(ActorSystemMessage): pass


class PendingActor(ActorSystemMessage):
    """Message sent to the local Admin to request an Actor be created that
    might need to be created in a remote ActorSystem.  An Actor sends
    this message when it is unable to create the child Actor directly
    (usually because of a capabilities mismatch).  If the ActorSystem
    Admin has a Convention partner that can supply the requested Actor
    functionality then it may issue a CreateActorForRemote to that
    convention partner.

    actorClassName

        The class name string of the new Actor to be started

    forActor

        The Actor which has requested the new Actor creation.  This
        will be None if requested externally via
        ActorSystem.createActor().

    instanceNumForActor

        The instance number from the forActor for this new Actor
        creation.  This is unique to the requesting forActor and helps
        the forActor map the original Local ActorAddress to the final
        useable ActorAddress.

    targetActorReq

        If specified, this is a picklable requirements object supplied
        by the creating Actor that is passed to the target Actor
        class's capability check to confirm that the target Actor will
        be able to provide the capabilities requested by the creating
        Actor.  The actual type of this object is opaque to the
        ActorSystem and it is simply supplied to the target Actor; the
        requesting Actor must specify targetActorReq in a form
        understood by the target Actor.

    globalName

        If not None, specifies the global name to register this Actor
        under within this ActorSystem.  If an Actor already exists
        under this name, that Actor's address is returned instead of
        creating a new Actor (and all other actor creation parameters
        are ignored).

    """
    def __init__(self, actorClassName, forActor, instanceNumForActor,
                 targetActorReq, globalName, sourceHash=None):
        self.actorClassName = actorClassName
        self.forActor       = forActor
        self.instanceNum    = instanceNumForActor
        self.targetActorReq = targetActorReq
        self.globalName     = globalName
        self.sourceHash     = sourceHash
        self.alreadyTried   = [] # array of convention actor addresses rejecting this request

    def __str__(self):
        return 'PendingActor#%d_of_%s'%(self.instanceNum, str(self.forActor)) + \
            ('is"%s"'%self.globalName if self.globalName else '')


class PendingActorResponse(ActorSystemMessage):
    ERROR_ActorSystem_Shutting_Down = 0xe01
    ERROR_No_Compatible_ActorSystem = 0xe02
    ERROR_Invalid_SourceHash        = 0xe03
    ERROR_Invalid_ActorClass        = 0xe04
    ERROR_Import                    = 0xe05

    def __init__(self, forActor, instanceNumForActor, globalName,
                 errorCode = None,
                 actualAddress = None,
                 errorStr = None):
        self.forActor      = forActor
        self.instanceNum   = instanceNumForActor
        self.errorCode     = errorCode  # False is no error
        self.actualAddress = actualAddress
        self.globalName    = globalName
        self.errorStr      = errorStr # supplemental to errorCode; may be blank

    def __str__(self):
        return 'PendingActorResponse(for %s inst# %s) errCode %s actual %s'%(
            self.forActor, self.instanceNum, self.errorCode, self.actualAddress)


class HandleDeadLetters(ActorSystemMessage):
    """Message sent to the Admin to register or de-register the specified
       address for DeadLetterBox handling."""
    def __init__(self, handlerAddr, enableHandler):
        self.handlerAddr   = handlerAddr
        self.enableHandler = enableHandler


class CapabilityUpdate(ActorSystemMessage):
    "Message sent to an ActorSystem with a capability update it should perform."
    def __init__(self, capabilityName, capabilityValue):
        self.capabilityName  = capabilityName
        self.capabilityValue = capabilityValue
    def __str__(self): return 'CapUpdate(%s = %s)'%(str(self.capabilityName),
                                                    str(self.capabilityValue))


class NewCapabilities(ActorSystemMessage):
    """Message sent by the ActorSystem to each Actor to notify the Actor
       of new capabilities.  This message is handled by the Actor
       Manager code and *never* passed to the Actor's
       .receiveMessage() method.  The Actor management code should
       check the new capabilities against the Actor's
       .actorSystemCapabilityCheck() method and if that method now
       returns false, the Actor should suicide via an ActorExitRequest
       message.
    """
    def __init__(self, newCapabilities, adminAddress):
        self.newCapabilities = newCapabilities
        self.adminAddress    = adminAddress


class RegisterSourceAuthority(ActorSystemMessage):
    "Sent to an ActorSystem to specify an Actor that will act as a Source Authority"
    def __init__(self, authorityAddress):
        self.authorityAddress = authorityAddress


class NotifyOnSourceAvailability(ActorSystemMessage):
    """Sent to an ActorSystem to specify an Actor that wants notifications
       of source loads and unloads
    """
    def __init__(self, notificationAddress, enable):
        self.notificationAddress = notificationAddress
        self.enable = enable
