"""Actor model execution framework (see https://en.wikipedia.org/wiki/Actor_model).

For more details, see http://thespianpy.com

Thespian Generation 2.6
"""

ThespianGeneration = (2, 6)


class ActorSystemException(Exception): pass

class InvalidActorAddress(ActorSystemException):
    '''Exception thrown if an ActorAddress is not valid (i.e. synthesized
    or altered) or could not be generated.'''
    def __init__(self, addr, desc, *args, **kw):
        self.actorAddress = addr
        ActorSystemException.__init__(self, str(addr) + ' is ' + desc, *args, **kw)


class ActorSystemFailure(ActorSystemException):
    "Exception thrown (in external app only) if ActorSystem request fails."


class ActorSystemStartupFailure(ActorSystemFailure):
    "Exception thrown (in external app only) if ActorSystem cannot startup"
    pass


class ActorSystemRequestTimeout(ActorSystemFailure):
    "Thrown if ActorSystem cannot complete request in stipulated or reasonable time period."
    pass


class NoCompatibleSystemForActor(ActorSystemException):
    "Thrown if Actor cannot be started because no ActorSystem matches the needed capabilities"
    def __init__(self, actorClass, msg, *args, **kw):
        ActorSystemException.__init__(self, msg + " for Actor {0}".format(str(actorClass)), *args, **kw)


class InvalidActorSourceHash(ActorSystemException):
    "Thrown on request to use a non-existent source hash."
    def __init__(self, badHash):
        ActorSystemException.__init__(self,
                                      'Source hash %s does not match any loaded sources.'%(
                                          badHash))

class InvalidActorSpecification(ActorSystemException):
    "Caller specified an invalid Actor Class for a createActor() request."
    def __init__(self, badActorClassSpecification):
        super(InvalidActorSpecification, self).__init__(
            'Invalid Actor Specification: %s'%str(badActorClassSpecification))



class ActorAddress:
    '''Used to reference a specific Actor (or Troupe).  The Actor could be
    local or remote, in another thread, or another process.  The actor
    is not even guaranteed to be alive anymore.  Regardless, this is
    the identifier by which messages can be directed to an Actor.
    '''
    def __init__(self, aaddr):
        '''Can only be constructed by the ActorSystem(); cannot be constructed
        or synthesized externally.

        The aaddr is meant to be an opaque reference generated
        internally by the ActorSystem implementation and used by it
        for routing purposes.  The user may request a string-version
        of the aaddr, but this should be used for informative or
        display purposes only.

        '''
        self._aaddr = aaddr

    @property
    def addressDetails(self): return self._aaddr

    @property
    def actorAddressString(self):
        if isinstance(self._aaddr, type("")):
            return self._aaddr
        return str(self._aaddr)

    def __str__(self): return 'ActorAddr-%s'%str(self._aaddr)

    def __eq__(self, o):
        if hasattr(self, 'eqOverride'):
            return self.eqOverride(o)
        try:
            return self.addressDetails == o.addressDetails
        except:
            return False
    def __ne__(self, o): return not self.__eq__(o)
    def __hash__(self):  return hash(self.addressDetails)


class Actor(object):
    '''This is the Actor encapsulation itself.  The receiveMessage()
       method should be overridden by the subclass to provide the
       message handling functionality.
    '''

    def __init__(self):
        """Called to initialize the Actor.

           Override this initialization method as needed in defined Actors.

           N.B.  Currently the Actor is not yet fully realized in the
           ActorSystem when __init__ is invoked.  This means that the
           Actor __init__ cannot invoke any ActorSystem-related
           operations (no .send(), .handleDeadLetters(),
           .notifyOnSystemRegistrationChanges(), etc.)

           Also note that there is post-__init__ processing of a
           created Actor object by the ActorSystem that is necessary
           for it to become a full Actor.  The Actor's __init__() must
           not perform Actor-related operations, and the __init__() is
           not sufficient to *fully* initialize an Actor object.  This
           ensures that the ActorSystem is involved in the creation of
           a useable Actor (i.e. the ActorSystem is the Factory for an
           Actor).

        """
        pass

    def __str__(self):
        return '{A:' + self.__class__.__name__ + ' @ ' + str(self.myAddress) + '}'

    def receiveMessage(self, msg, sender):
        '''Main entry point handling a request received by this Actor.  Runs
           without interruption and may access locals to this Actor
           (only) without concern that these locals will be modified
           externally.
        '''
        noPyLintWarnings = msg, sender
        assert False, \
            'default Actor.receiveMessage for "%s" must be overridden to handle messages'%self

    @property
    def myAddress(self):
        "Returns the ActorAddress of this Actor itself."
        return self._myRef.address

    def createActor(self, actorClass, targetActorRequirements=None, globalName=None, sourceHash = None):
        """Initiates creation of a new child Actor of the specified Class.  Returns the ActorAddress for that child
           Actor.

           If the optional globalName parameter is specified, the
           ActorSystem will first check for a registered Actor under
           that name and return that Actor's address if one is
           registered; in this case all other arguments to createActor
           are ignored (and the registered Actor's parent is not
           necessarily the current Actor).  If no Actor is registered
           under that name, the requested Actor is created and
           registered under that name.
        """
        return self._myRef.createActor(actorClass, targetActorRequirements, globalName, sourceHash)

    def send(self, targetAddr, msg):
        """Sends a message to another Actor (specified via ActorAddress) from
           this Actor.  The msg must be pickle-able."""
        if not isinstance(targetAddr, ActorAddress):
            raise InvalidActorAddress(targetAddr,
                                      'not a valid ActorAddress for sending messages to')
        self._myRef.actor_send(targetAddr, msg)

    def wakeupAfter(self, timePeriod):
        "Requests delivery of a WakeupMessage after the specified period of time."
        self._myRef.wakeupAfter(timePeriod)

    def handleDeadLetters(self, startHandling=True):
        """Registers this Actor with the ActorSystem as a recipient of
           DeadLetters.  If multiple Actors register, the DeadLetter
           is passed to each actor.  If the optional argument is
           False, or if this Actor exits, then it is automatically
           removed from Dead Letter handling. """
        self._myRef.handleDeadLetters(self.myAddress, startHandling)

    def registerSourceAuthority(self):
        """Registers this Actor as the Source Authority for authorizing (and
           decrypting) loadActorSource() inputs.
        """
        self._myRef.registerSourceAuthority(self.myAddress)

    def notifyOnSystemRegistrationChanges(self, startHandling=True):
        """Registers this Actor with the ActorSystem as a recipient of 
           ActorSystemConventionUpdate messages."""
        self._myRef.notifyOnSystemRegistrationChanges(self.myAddress, startHandling)

    def logger(self, name=None):
        return self._myRef.logger(name)

    def updateCapability(self, capabilityName, capabilityValue=None):
        """Updates the specified capability for the current Actor System
           hosting this Actor to have the newly specified value.  This
           may cause other Actors to be stopped or restarted if they
           depended upon the capability being modified.  Capabilities
           set to None will be removed.
        """
        self._myRef.updateCapability(capabilityName, capabilityValue)

    def loadActorSource(self, fname):
        """Loads the specified file as a new source containing Actor code
           (subject to validation by any loaded Source Authority (see
           the registerSourceAuthority() call).  Returns the source
           hash associated with the loaded source; this hash can be
           used in the createActor() and unloadActorSource() calls.
        """
        return self._myRef.loadActorSource(fname)

    def unloadActorSource(self, sourceHash):
        """Unloads the previously loaded source specified by the sourceHash.
           If the specified hash does not match any currently loaded
           sources then this operation will be ignored.
        """
        return self._myRef.unloadActorSource(sourceHash)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Most actors will not use the below, but they can be used
    # to influence the Convention membership for Thespian

    def preRegisterRemoteSystem(self, remoteAddress, remoteCapabilities):
        """Called to indicate that a remote Actor System at the specified
           address is expected to be part of the Convention.  The
           remoteAddress is interpreted by the current Actor System
           base and if it is appropriate to the currently running
           system base, the specified remote will be added to the
           current convention with the specified initial capabilities,
           but in a non-attached state.  The remote Actor System
           cannot actually be used until the local Actor System can
           communicate with the remote Actor System.

           Most Actors should not involve themselves in Actor System
           registration, but this can be useful for Actor Systems that
           can only send outbound messages to inform them of remote
           systems they should initiate connectivity to.
        """
        self._myRef.preRegisterRemoteSystem(remoteAddress, remoteCapabilities)

    def deRegisterRemoteSystem(self, remoteAddress):
        """Called to indicate that a previously registered remote Actor System
           should be removed from registration.  This is usually used
           when the remote has been shutdown and is not expected to
           return.  This should normally only be used for systems
           previously described via the preRegisterRemoteSystem()
           call.  This removed state is not enforced: if the remote
           system initiates connectivity with the local system after
           this call then it will be re-entered into the Convention.
        """
        self._myRef.deRegisterRemoteSystem(remoteAddress)



class ActorSystemMessage(object):
    "Base class for all ActorSystem Messages for easier isinstance identification"
    pass


class ActorSystemConventionUpdate(ActorSystemMessage):
    """This message is delivered to actors that have registered for
       ActorSystem Convention updates via their
       notifyOnSystemRegistrationChanges calls.
    """
    def __init__(self, adminAddress, capabilities = None, added = True):
        self._remoteAdminAddress = adminAddress
        self._capabilities       = capabilities
        self._added              = added # if False, remote system actively de-registered

    @property
    def remoteAdminAddress(self): return self._remoteAdminAddress
    @property
    def remoteCapabilities(self): return self._capabilities
    @property
    def remoteAdded(self): return self._added
    def __eq__(self, o):
        return isinstance(o, ActorSystemConventionUpdate) \
            and self._remoteAdminAddress == o._remoteAdminAddress \
            and self._capabilities == o._capabilities \
            and self._added == o._added
    def __ne__(self, o): return not self.__eq__(o)


class ActorExitRequest(ActorSystemMessage):
    """This message should be sent to an Actor to request that it be
       shutdown.  After processing this message, the ActorSystem will
       terminate this Actor instance.  An Actor can stop itself by
       sending this message to itself.

       If recursive is True (the default) then this exit request will
       be forwarded to all the Actor's known children as well.  The
       default of True (explicitly set during ActorSystem shutdown)
       helps ensure that orphaned Actor processes are not left
       running; the recursive send to the children is handled by the
       ActorSystem after delivering the ActorExitRequest to the parent
       actor and before completing its shutdown.  The Parent Actor can
       clear this flag if it does not want the shutdown request
       propagated to its children by the ActorSystem.

    """
    def __init__(self, recursive=True):
        self._recursive = recursive

    def __str__(self): return 'ActorExitRequest'
    def __eq__(self, o): return isinstance(o, ActorExitRequest)
    def __ne__(self, o): return not self.__eq__(o)

    @property
    def isRecursive(self): return self._recursive
    def notRecursive(self): self._recursive = False


class ChildActorExited(ActorSystemMessage):
    """This is a message sent to any parent Actor (sender is the
       ActorSystem) when one of its child Actors exits.
    """
    def __init__(self, exitedChildAddress):
        self._childAddr = exitedChildAddress

    def __str__(self):
        return 'ChildActorExited:' + str(self._childAddr)
    def __eq__(self, o): return isinstance(o, ChildActorExited) and self._childAddr == o._childAddr
    def __ne__(self, o): return not self.__eq__(o)

    @property
    def childAddress(self):
        "Used by the parent Actor to get the address of the exited child"
        return self._childAddr


class PoisonMessage(ActorSystemMessage):
    """Message wrapper used to return a message to the sender that has
       caused multiple failures in a recipient Actor."""
    def __init__(self, poison):
        self._poison = poison
    @property
    def poisonMessage(self):
        "Returns the actual message that poisoned the target Actor."
        return self._poison
    def __str__(self): return 'Poison<%s>'%str(self.poisonMessage)
    def __eq__(self, o): return isinstance(o, PoisonMessage) and self._poison == o._poison
    def __ne__(self, o): return not self.__eq__(o)


class WakeupMessage(ActorSystemMessage):
    """Message sent as a result of a .wakeupAfter() call.  The .delayPeriod value is the amount of time of the delay."""
    def __init__(self, delayPeriod):
        self.delayPeriod = delayPeriod
    def __str__(self):
        return 'WakeupMessage(%s)'%str(self.delayPeriod)
    def __eq__(self, o): return isinstance(o, WakeupMessage) and self.delayPeriod == o.delayPeriod
    def __ne__(self, o): return not self.__eq__(o)


class ThespianWatch(object):
    """If an Actor's receiveMessage returns this object, it specifies a
    list of fileno's to watch for activity/availability (in addition
    to the normal mailbox for the Actor).  If any of these fileno's
    become ready, the Actor's receiveMessage will be called with a
    WatchMessage and a list of those ready fileno's.

    Support for watch capability is dependent on the system base and
    hosting operating system.  ...
    """
    def __init__(self, filenos):
        self.filenos = filenos

class WatchMessage(ActorSystemMessage):
    """Message sent to an Actor with a subset of the filenos in the
       ThespianWatch that are ready/active"""
    def __init__(self, ready):
        self.ready = ready


class DeadEnvelope(ActorSystemMessage):

    """Envelope for a message that was addressed to an address that's now
       dead.  This message should be routed to the dead letter
       handler.
    """
    def __init__(self, origTgt, origMsg):
        self.deadMessage = origMsg
        self.deadAddress = origTgt

    def __str__(self):
        if id(self.deadMessage) == id(self):
            return 'Self-referential DeadEnvelope!'
        if isinstance(self.deadMessage, DeadEnvelope):
            if id(self.deadMessage.deadMessage) == id(self):
                return 'Self-referential-once-removed DeadEnvelope!'
        return 'DeadEnvelope(%s)->%s'%(str(self.deadMessage), str(self.deadAddress))
    def __eq__(self, o):
        return isinstance(o, DeadEnvelope) \
            and self.deadMessage == o.deadMessage \
            and self.deadAddress == o.deadAddress
    def __ne__(self, o): return not self.__eq__(o)


class ValidateSource(ActorSystemMessage):
    "Provides loadActorSource input that should be validated (and possibly decrypted)."
    def __init__(self, sourceHash, sourceData):
        self.sourceHash = sourceHash
        self.sourceData = sourceData
    def __eq__(self, o): return isinstance(o, ValidateSource) and self.sourceHash == o.sourceHash
    def __ne__(self, o): return not self.__eq__(o)


class ValidatedSource(ActorSystemMessage):
    "The response to the ValidateSource providing the validated source code to enable."
    def __init__(self, sourceHash, sourceZip):
        self.sourceHash = sourceHash
        self.sourceZip  = sourceZip
    def __eq__(self, o): return isinstance(o, ValidatedSource) and self.sourceHash == o.sourceHash
    def __ne__(self, o): return not self.__eq__(o)


class ActorSystem(object):
    """Defines the Actor System and external interface operations.

    When initializing, the systemBase can be specified to indicate the
    underlying environment in which the ActorSystem should operate.
    The systemBase can be a string indicating a known base system
    type, or it can be a SystemBase object created by the user.  The
    default SystemBase is the previously specified systemBase, or the
    procSystemBase.ActorSystemBase if this is the first instantiation of
    the ActorSystem.

    Although multiple ActorSystems may be instantiated, it is intended
    that there is only one for the entire application, and that the
    base is specified on the first call only, with subsequent calls
    passing the same base or no base specification (in which case the
    ActorSystem instances will act as a global singleton).  It is
    possible to instantiate different ActorSystems, but the first one
    acts as the Singleton and the others are separate (and may cause
    execution conflicts).

    When injecting messages into the ActorSystem from the outside
    (i.e. not from Actors) the .tell() and .ask() methods should be
    used.  The .tell() method will send a message to the specified
    Actor.  The .ask() method will send a message and then suspend the
    current thread to await a response.

    New Actors can be created from the outside by calling the
    createActor() method; Actors wishing to create new Actors should
    use the .createActor() method on the Actor object.

    A global actor name may be specified as an optional parameter on
    the createActor() method.  This can be used to assign a specific
    name to a specific Actor across the entire ActorSystem.  If there
    is already an Actor registered with this global name, no new actor
    is created and the address of the existing actor is returned.
    There is no other regulation of this namespace and it is
    recommended that it be used sparingly and only for top-level
    Actors that need to be singletons.  Note specifying a globalName
    of an Actor that already exists causes all other createActor
    parameters to be ignored.

    """

    def __init__(self,
                 systemBase = None,
                 capabilities = None,
                 logDefs = None,
                 transientUnique = False):
        systemBase = self._startupActorSys(None if transientUnique
                                           else getattr(self.__class__, 'systemBase', None),
                                           systemBase, capabilities, logDefs)
        if transientUnique:
            self._isTransientUnique = True
        else:
            # (Re-)Set the Singleton systemBase
            self.__class__.systemBase = systemBase


    def _startupActorSys(self, currentSystemBase, systemBase, capabilities, logDefs):
        self.systemAddress = ActorAddress('/ActorSys')
        self.capabilities = capabilities or dict()
        if 'logging' in self.capabilities:
            import logging
            logging('Thespian').warning('logging specification moved from capabilities to an explicit argument.')
        if systemBase is None:
            systemBase = currentSystemBase
            if systemBase is None:
                import thespian.system.simpleSystemBase
                systemBase = thespian.system.simpleSystemBase.ActorSystemBase(self, logDefs = logDefs)
        elif isinstance(systemBase, str):
            import sys
            if sys.version_info < (2,7):
                import thespian.importlib as importlib
            else:
                import importlib
            # n.b. let standard import exception indicate a missing/unknown systemBase
            module = importlib.import_module('thespian.system.%s'%systemBase)
            sbc = getattr(module, 'ActorSystemBase')
            if currentSystemBase and id(currentSystemBase.__class__) == id(sbc):
                systemBase = currentSystemBase
            else:
                systemBase = sbc(self, logDefs = logDefs)
        elif systemBase and currentSystemBase:
            if id(systemBase.__class__) == id(currentSystemBase.__class__):
                systemBase = currentSystemBase
        # else systemBase should be a valid object already
        self._systemBase = systemBase
        return systemBase


    def shutdown(self):
        "Called to shutdown the ActorSystem itself.  May block until all Actors are shutdown."
        if self._systemBase: self._systemBase.shutdown()
        if not getattr(self, '_isTransientUnique', False):
            if getattr(self.__class__, 'systemBase', None) == self._systemBase:
                delattr(self.__class__, 'systemBase')
        self._systemBase = None


    def createActor(self, actorClass,
                    targetActorRequirements=None,
                    globalName=None,
                    sourceHash=None):
        'Called to create a "Primary" Actor (a top-level Actor owned by the system itself).'
        return self._systemBase.newPrimaryActor(actorClass, targetActorRequirements,
                                                globalName, sourceHash)

    def tell(self, actorAddr, msg):
        "Sends msg to the Actor at the specified address.  No response is expected or awaited."
        if not isinstance(actorAddr, ActorAddress):
            raise ValueError('Actor tell address is not a valid ActorAddress: %s'%(type(actorAddr)))
        self._systemBase.tell(actorAddr, msg)

    def listen(self, timeout=None):
        """Waits for a message from any Actor.  The optional timeout argument
        specifies the maximum amount of time to wait in fractional
        seconds.  Returns None if no response is received in the
        indicated time period.
        """
        return self._systemBase.listen(timeout)

    def ask(self, actorAddr, msg, timeout=None):
        """Sends msg to the addressed Actor and waits for a response (from
        *any* Actor).  The optional timeout argument specifies the
        maximum amount of time to wait in fractional seconds.  Returns
        None if no response is received in the indicated time period.
        """
        if not isinstance(actorAddr, ActorAddress):
            raise ValueError('Actor ask address "%s" is not a valid ActorAddress'%str(actorAddr))
        return self._systemBase.ask(actorAddr, msg, timeout)

    def _handleDeadLetters(self, address, enable):
        self._systemBase._handleDeadLetters(address, enable)

    def systemUpdate(self, updateType, *updateArgs, **updateKWArgs):
        """Back door to allow access to the underlying systemBase
           implementation.  Implementations may differ, so this is not
           generally recommended and is intended for unusual
           circumstances like unit test controls, etc.
        """
        return getattr(self._systemBase, updateType, lambda *a, **kw: None)(*updateArgs, **updateKWArgs)

    def updateCapability(self, capabilityName, capabilityValue=None):
        "Adds/modifies an ActorSystem capability (or removes it if the value is None or not specified)."
        self._systemBase.updateCapability(capabilityName, capabilityValue)
        if capabilityValue is None:
            if capabilityName in self.capabilities:
                del self.capabilities[capabilityName]
        else:
            self.capabilities[capabilityName] = capabilityValue


    def loadActorSource(self, fname):
        return self._systemBase.loadActorSource(fname)


    def unloadActorSource(self, sourceHash):
        return self._systemBase.unloadActorSource(sourceHash)



def requireCapability(cap, value=True):
    '''Actor class decorator for requiring a capability.'''
    def go(cls):
        capCheck0 = None
        if hasattr(cls, 'actorSystemCapabilityCheck'):
            capCheck0 = cls.actorSystemCapabilityCheck
        @staticmethod
        def capCheck1(caps, reqs):
            return caps.get(cap, False) == value and (capCheck0(caps,reqs)
                                                      if capCheck0 else True)
        cls.actorSystemCapabilityCheck = capCheck1
        return cls
    return go


import inspect

class ActorTypeDispatcher(Actor):
    """This is an enhancement on the base Actor where the receiveMessage
       determines the type of the received message and calls a
       "receiveMsg_{type}" method (if it exists) to handle it.  The
       specific type of the message is checked, then the parent of the
       message type, all the way back through to the base message
       type.

       This processing will check subclasses first for the method.  It
       is not possible to perform the normal "return super(MYCLASS,
       self).foo()" if the current class does not handle the message
       and it should be passed to a parent class; in part because the
       parent class may handle the message as a parent of the message
       (there are two class heirarchies being checked: the Actor's and
       the message's).  To enable the proper functionality, the
       "receiveMsg_{type}" method should return self.SUPER if it has
       not handled the message and it (or a base class of it) should
       be passed to a base class handler.

    """

    SUPER = hash("SUPER")

    def receiveMessage(self, message, sender):
        for each in inspect.getmro(message.__class__):
            methodName = 'receiveMsg_%s'%each.__name__
            if hasattr(self, methodName):
                for klasses in inspect.getmro(self.__class__):
                    if hasattr(klasses, methodName):
                        rval = getattr(klasses, methodName)(self, message, sender)
                        if rval != self.SUPER:
                            return rval
        if hasattr(self, 'receiveUnrecognizedMessage'):
            return self.receiveUnrecognizedMessage(message, sender)


from thespian.system.messages.status import (Thespian_StatusReq,
                                             Thespian_SystemStatus,
                                             Thespian_ActorStatus)
