import types
from thespian.actors import ActorAddress, DeadEnvelope, ChildActorExited
from thespian.system.utilis import thesplog
from thespian.system.transport import SendStatus
import logging


class ActorLocalAddress:
    """Used as the detail for an ActorAddress where the ActorAddress was
       generated locally by an Actor for a created child Actor in lieu
       of the actual final address of that child Actor.  This object
       defines the target ActorAddress in context of the Actor which
       generated that address, and is only valid for use by that
       generating Actor.  As such, it is not itself pickleable
       (although it may have a useable equivalent that can be
       substituted when pickling).
    """
    def __init__(self, generatingActorAddress, addressInstanceNum, addrManager):
        self.generatingActor    = generatingActorAddress
        self.addressInstanceNum = addressInstanceNum
        self.addressManager     = addrManager
    def __eq__(self, o):
        # n.b. compareAddressEq below expects this to throw an exception if o is not an ActorLocalAddress
        return isinstance(o, ActorLocalAddress) and self.generatingActor == o.generatingActor and self.addressInstanceNum == o.addressInstanceNum
    def __ne__(self, o): return not self.__eq__(o)
    def __str__(self):
        try:
            realized = self.addressManager.exportAddr(self)
            if realized:
                return str(realized)
        except Exception:
            pass
        return 'LocalAddr.%s'%self.addressInstanceNum


class CannotPickle(Exception):
    """This exception is thrown for objects that explicitly reject the
       effort to pickle them for sending them to a remote Actor."""
    pass


class CannotPickleAddress(Exception):
    """This exception is thrown for unpickleable ActorAddress objects.
       This is usually because it's a local address without, as yet, a
       useable translation.  This send might be retried later when the
       remote Actor Address is known.
    """
    def __init__(self, address, *args, **kw):
        super(CannotPickleAddress, self).__init__(*args, **kw)
        self.address = address


def _pickle_if_translation(lclAddr):
    """Assigned to ActorAddresses of ActorLocalAddress details.  Prevents
       pickling unless there is a useable translation, in which case
       that translation is the pickled version.
    """
    useableAddr = lclAddr.addressDetails.addressManager.exportAddr(lclAddr)
    if useableAddr:
        return useableAddr.__getstate__() \
            if hasattr(useableAddr, '__getstate__') else useableAddr.__dict__
    raise CannotPickleAddress(lclAddr, 'ActorLocalAddress %s cannot be Pickled.', str(lclAddr))


def _pickle_clean(dirtyAddr):
    "Cleans up an ActorAddress of any special, unpickleable elements so it can be pickled."
    cd = dirtyAddr.__dict__.copy()
    for rmv in [ 'eqOverride', '__getstate__', '_pickle_clean']:
        if rmv in cd:
            del cd[rmv]
    return cd


class ActorAddressManager:
    """Class used to manage ActorLocalAddress addresses and their
       translation to a directly-useable ActorAddress."""
    def __init__(self, adminAddress, thisActorAddress):
        self._adminAddr = adminAddress
        self._thisActorAddr = thisActorAddress  # managing address on behalf of this Actor  # change to myAddr
        self._managed = []  # value = None or directly-useable Address
        self._deadAddrs = [] # value = dead ActorAddress

    def createLocalAddress(self):
        """Creates a new ActorLocalAddress-based ActorAddress.  A useable
           Address will be associated with this address in the future."""
        self._managed.append(None)
        return self.getLocalAddress(len(self._managed) - 1)

    def getLocalAddress(self, instanceNum):
        'Returns ActorAddress corresponding to local address instance'
        ra =  ActorAddress(ActorLocalAddress(self._thisActorAddr, instanceNum, self))
        ra.eqOverride = types.MethodType(self.compareAddressEq, ra)
        ra.__getstate__ = types.MethodType(_pickle_if_translation, ra)
        return ra

    def importAddr(self, anAddr):
        """Called for any useable Address being passed to the Actor; ensures
           that any comparison between this useable Address and a
           local Address created originally will be properly
           performed.
        """
        anAddr.eqOverride = types.MethodType(self.compareAddressEq, anAddr)
        anAddr.__getstate__ = types.MethodType(_pickle_clean, anAddr)
        if anAddr in self._deadAddrs:
            del self._deadAddrs[self._deadAddrs.index(anAddr)]

    def deadAddress(self, address):
        """This function is called to track a known dead address.  This method
           should be used by transports that cannot independently
           determine that an address is dead (e.g. multiprocQueueBase)
           but it should *not* be used for transports that may re-use
           addresses (e.g. multiprocTCPBase re-use of port
           numbers).
        """
        self._deadAddrs.append(address)
        # If the following is present, then the index of actors
        #changes after a dead address, which causes subsequent
        #PendingActor completions to attach to the wrong address or an
        #invalid index.  With it here though, the _managed never
        #shrinks.  Ever.  self._managed = filter(lambda a: a !=
        #address, self._managed)

    def isDeadAddress(self, address):
        return address in self._deadAddrs

    def remove_dead_address(self, address):
        if address in self._deadAddrs:
            self._deadAddrs.remove(address)

    def associateUseableAddress(self, ownerAddress, ownerInstance, useableAddress):
        """Called when the actual Actor Address becomes known for a
           locally-generated Address (e.g. when the remote Actor has
           been created and addressed).  Associates the local address
           with the remote address for future use.
        """
        if ownerAddress == self._thisActorAddr:
            if len(self._managed) > ownerInstance:
                # Allows updates
                self._managed[ownerInstance] = useableAddress
        self.importAddr(useableAddress)


    def compareAddressEq(self, addr1, addr2):
        "Checks if two addresses are equal, considering local/useable associations"
        if addr1 is None: return addr2 is None
        if addr2 is None: return False
        try:
            if addr1.addressDetails == addr2.addressDetails:
                return True
        except AttributeError:
            # Cannot directly compare address details elements; try more sophisticated checks
            pass
        if isinstance(addr1, ActorAddress) and \
           isinstance(addr1.addressDetails, ActorLocalAddress):
            if isinstance(addr2, ActorAddress) and \
               isinstance(addr2.addressDetails, ActorLocalAddress):
                return addr1.addressDetails == addr2.addressDetails
            try:
                return (addr1.addressDetails.generatingActor == self._thisActorAddr and
                        self._managed[addr1.addressDetails.addressInstanceNum] and
                        self._managed[addr1.addressDetails.addressInstanceNum].addressDetails ==
                        addr2.addressDetails)
            except AttributeError:
                # Cannot compare these details... must not be equivalent addresses
                return False
        if isinstance(addr2, ActorAddress) and \
           isinstance(addr2.addressDetails, ActorLocalAddress):
            try:
                return (addr2.addressDetails.generatingActor == self._thisActorAddr and
                        self._managed[addr2.addressDetails.addressInstanceNum] and
                        self._managed[addr2.addressDetails.addressInstanceNum].addressDetails ==
                        addr1.addressDetails)
            except AttributeError:
                # Cannot compare these details... must not be equivalent addresses
                return False
        # IncomPARable!!
        return False


    def exportAddr(self, anAddress):
        """Returns an exportable form of the Address: internal addresses are
           converted to external; if no conversion is yet possible,
           this returns None.  Input is either an ActorAddress or the
           details of an actor address whose public export version is
           to be looked up and returned.

        """
        details = getattr(anAddress, 'addressDetails', anAddress)
        if isinstance(details, ActorLocalAddress):
            if details.generatingActor == self._thisActorAddr:
                return self._managed[details.addressInstanceNum]
            return None
        # Assumed to be directly useable
        return anAddress


    def sendToAddress(self, anAddress):
        """Returns an ActorAddress useable for sending messages to, or None if
           not available yet.  This is very similar to exportAddr
           except that this method converts a dead target address into
           the Admin address for dead letter forwarding.

        """
        return self.exportAddr(anAddress)


    def prepMessageSend(self, anAddress, msg):
        """Prepares to send the specified message to the specified address,
           returning a tuple of the send-to-address and the (possibly
           updated) message to send.

           The address may be converted from an internal to an
           exportable address.

           If the target address is known as a dead letter box, the
           Admin address is returned instead and the message is
           wrapped in a DeadEnvelope wrapper.

           If the target address is not ready for use, the
           send-to-address portion of the tuple will return a value of
           None.

           If the message should no longer be sent, the message
           portion of the tuple will be returned as
           SendStatus.DeadTarget (because this is *never* a valid
           message to actually send).

           n.b. this method may or may not be called while holding a
           lock.  It is a lookup-only operation, so the lock should
           not be of any consequence.
        """
        tgtaddr = self.exportAddr(anAddress)
        if tgtaddr is None:
            return None, msg
        if tgtaddr in self._deadAddrs:
            if isinstance(msg, (DeadEnvelope, ChildActorExited)):
                thesplog('Discarding %s to %s because the latter is dead.',
                         str(msg), str(tgtaddr))
                return None, SendStatus.DeadTarget
            return self._adminAddr, DeadEnvelope(anAddress, msg)
        return tgtaddr, msg
