"""Uses the python multiprocess.Queue as the transport mechanism.

Queues are multi-producer/multi-consumer objects.  In this usage,
there will be only one consumer (the current Actor) although there may
be multiple producers (any other actor sending to this actor); this
actor has a single Queue for all incoming messages this actor will
handle.

However, Queues can only be passed by inheritance, not by pickling, so
only the Parent Actor has the proper queue handle to talk to the Child
Actor.  Any other Actor wishing to talk to the Child Actor will have
received the Child Actor's address from the Parent Actor, and that
address will indicate that the message must be forwarded through the
Parent Actor.  By extension, passing an Actor address itself must be
forwarded through the parent, so this means that a message passed from
one Actor to another must be passed up from the sender to it's Parent
and the message will recurse up to the Parent that is common to both
the Sender and the Receiver, where it will then traverse down the
chain of children to the destination Actor.

This also requires Parent Actors to learn about actors their Children
have created.  Each Actor therefore will maintain a table of known
addresses.  Any time an Actor creates a new child Actor, it will send
the Address of that Child Actor up to it's Parent, which will record
the new Address and the creating Child for future forwarding to that
Address.  The Parent will then recursively send the address upwards;
the Admin will end up having a table of all known Actors.

Worst case is when an actor address has been passed inside a message,
so a particular Actor has no idea how to route an address.  If this
routing deferall propagates up to the Admin, then the message will be
sent to EACH AND EVERY child (on the assumption that it will be
discarded by all leaves except the ultimate recipient).  Very
inefficient.

This transport is therefore not the most efficient transport, but it
is used as a semi-academic exercise to ensure that the Thespian system
remains flexible to handle non-socket transport mechanisms.

2014-Nov-01 NOTE: this actor seems to be prone to deadlock; multiple
actors telling messages (or external telling internal) appears to
deadlock often.  This includes testLoad non-asking tests (deadlock
100% of the time) and
testActorAdder.py:test07_LotsOfActorsEveryTenWithBackground (deadlocks
25% of the time).

"""


import logging
from thespian.actors import *
from thespian.system.utilis import ExpiryTime, thesplog, partition, foldl
from thespian.system.transport import *
from thespian.system.transport.asyncTransportBase import asyncTransportBase
from thespian.system.transport.wakeupTransportBase import wakeupTransportBase
from thespian.system.addressManager import ActorLocalAddress
from multiprocessing import Queue
try:
    import Queue as Q  # Python 2
except ImportError:
    import queue as Q  # Python 3
from datetime import datetime
import pickle


MAX_ADMIN_QUEUESIZE=40  # depth of Admin queue
MAX_ACTOR_QUEUESIZE=10  # depth of Actor queue


class QueueActorAddress(object):  # internal use by this module only
    def __init__(self, name):
        self._qaddr = name
    def __str__(self):   return 'Q.'+str(self._qaddr)
    def __eq__(self, o): return isinstance(o, QueueActorAddress) and self._qaddr == o._qaddr
    def __ne__(self, o): return not self.__eq__(o)
    def __hash__(self): return hash(str(self))
    def subAddr(self, inst):
        import string
        useable = string.ascii_letters
        namegen = lambda v: addnext(*divmod(v, len(useable)))
        addnext = lambda x,y: useable[y] if 0 == x else (namegen(x) + useable[y])
        return self._qaddr + '.' + namegen(inst)


class MpQTEndpoint(TransportInit__Base):  # internal use by this module only
    def __init__(self, *args): self.args = args
    @property
    def addrInst(self): return self.args[0]


class MultiprocessQueueTransport(asyncTransportBase, wakeupTransportBase):
    """A transport designed to use a multiprocess.Queue instance to send
       and receive messages with other multiprocess Process actors.
       There is one instance of this object in each Actor.  This
       object maintains a single input queue (used by its parent an
       any children it creates) and a table of all known sub-Actor
       addresses and their queues (being the most immediate child
       Actor queue that moves the message closer to the child target,
       or the Parent actor queue to which the message should be
       forwarded if no child is identified).
    """

    def __init__(self, initType, *args):
        super(MultiprocessQueueTransport, self).__init__()
        if isinstance(initType, ExternalInterfaceTransportInit):
            # External process that's going to talk "in".  There is no
            # parent, and the child is the systemAdmin.
            capabilities, logDefs = args
            self._parentQ         = None
            self._adminQ          = Queue(MAX_ADMIN_QUEUESIZE)
            self._adminAddr       = self.getAdminAddr(capabilities)
            self._myQAddress      = ActorAddress(QueueActorAddress('~'))
            self._myInputQ        = Queue(MAX_ACTOR_QUEUESIZE)
        elif isinstance(initType, MpQTEndpoint):
            _addrInst, myAddr, myQueue, parentQ, adminQ, adminAddr = initType.args
            self._parentQ    = parentQ
            self._adminQ     = adminQ
            self._adminAddr  = adminAddr
            self._myQAddress = myAddr
            self._myInputQ   = myQueue
        else:
            thesplog('MultiprocessQueueTransport init of type %s unsupported!', str(initType),
                     level=logging.ERROR)

        # _queues is a map of direct child ActorAddresses to Queue instance.  Note
        # that there will be multiple keys mapping to the same Queue
        # instance because routing is only either to the Parent or to
        # an immediate Child.
        self._queues = {}

        # _fwdvia represents routing for other than immediate parent
        # or child (there may be multiple target addresses mapping to
        # the same forward address.
        self._fwdvia = {} # key = targetAddress, value=fwdViaAddress

        self._nextSubInstance = 0


    def protectedFileNumList(self):
        return foldl(lambda a, b: a+[b._reader.fileno(), b._writer.fileno()],
                     [self._myInputQ, self._parentQ, self._adminQ] +
                     list(self._queues.values()), [])

    def childResetFileNumList(self):
        return foldl(lambda a, b: a+[b._reader.fileno(), b._writer.fileno()],
                     [self._parentQ] +
                     list(self._queues.values()), [])


    @property
    def myAddress(self): return self._myQAddress


    @staticmethod
    def getAddressFromString(addrspec):
        # addrspec is assumed to be a valid address string
        return ActorAddress(QueueActorAddress(addrspec))

    @staticmethod
    def getAdminAddr(capabilities):
        return MultiprocessQueueTransport.getAddressFromString(
            capabilities.get('Admin Address', 'ThespianQ'))


    @staticmethod
    def probeAdmin(addr):
        """Called to see if there might be an admin running already at the
           specified addr.  This is called from the systemBase, so
           simple blocking operations are fine.  This only needs to
           check for a responder; higher level logic will verify that
           it's actually an ActorAdmin suitable for use.
        """
        # never reconnectable; Queue objects are only available from
        # the constructor and cannot be synthesized or passed.
        return False


    def _updateStatusResponse(self, resp):
        "Called to update a Thespian_SystemStatus or Thespian_ActorStatus with common information"
        asyncTransportBase._updateStatusResponse(self, resp)
        wakeupTransportBase._updateStatusResponse(self, resp)


    def _nextSubAddress(self):
        subAddrStr = self._myQAddress.addressDetails.subAddr(self._nextSubInstance)
        self._nextSubInstance = self._nextSubInstance + 1
        return ActorAddress(QueueActorAddress(subAddrStr))


    def prepEndpoint(self, assignedLocalAddr, capabilities):
        """In the parent, prepare to establish a new communications endpoint
           with a new Child Actor.  The result of this call will be
           passed to a created child process to use when initializing
           the Transport object for that class; the result of this
           call will also be kept by the parent to finalize the
           communications after creation of the Child by calling
           connectEndpoint() with this returned object.
        """
        if isinstance(assignedLocalAddr.addressDetails, ActorLocalAddress):
            return MpQTEndpoint(assignedLocalAddr.addressDetails.addressInstanceNum,
                                self._nextSubAddress(),
                                Queue(MAX_ACTOR_QUEUESIZE),
                                self._myInputQ, self._adminQ, self._adminAddr)
        return MpQTEndpoint(None,
                            assignedLocalAddr,
                            self._adminQ, self._myInputQ, self._adminQ, self._adminAddr)

    def connectEndpoint(self, endPoint):
        """Called by the Parent after creating the Child to fully connect the
           endpoint to the Child for ongoing communications."""
        _addrInst, childAddr, childQueue, _myQ, _adminQ, _adminAddr = endPoint.args
        self._queues[childAddr] = childQueue


    def deadAddress(self, addressManager, childAddr):
        if childAddr in self._queues:
            # Can no longer send to this Queue object.  Delete the
            # entry; this will cause forwarding of messages, although
            # the addressManager is also aware of the dead address and
            # will cause DeadEnvelope forwarding.  Deleting here
            # prevents hanging on queue full to dead children.
            del self._queues[childAddr]
        deadfwd, okfwd = ([],[]) if False else \
                         partition(lambda i: i[0] == childAddr or i[1] == childAddr,
                                   self._fwdvia.items())
        if deadfwd:
            self._fwdvia = dict(okfwd)
        super(MultiprocessQueueTransport, self).deadAddress(addressManager, childAddr)


    def _runWithExpiry(self, incomingHandler):
        """Core scheduling method; called by the current Actor process when
           idle to await new messages (or to do background
           processing).
        """
        if incomingHandler == TransmitOnly or \
           isinstance(incomingHandler, TransmitOnly):
            # transmits are not queued/multistage in this transport, no waiting
            return 0

        self._aborting_run = False

        while not self.run_time.expired() and not self._aborting_run:
            try:
                rcvd = self._myInputQ.get(True, self.run_time.remainingSeconds())
            except Q.Empty:
                # Probably a timeout, but let the while loop decide for sure
                continue
            relayAddr, (sendAddr, destAddr, msg) = rcvd
            if sendAddr not in self._queues:
                # We don't directly know about this sender, so
                # remember what path this arrived on to know where to
                # direct future messages for this sender.
                if relayAddr and relayAddr in self._queues and \
                   sendAddr not in self._fwdvia:
                    # relayAddr might be None if it's our parent, which is OK because
                    # the default message forwarding is to the parent.  If it's not
                    # none, it should be in self._queues though!
                    self._fwdvia[sendAddr] = relayAddr
            if hasattr(self, '_addressMgr'):
                destAddr,msg = self._addressMgr.prepMessageSend(destAddr, msg)
            if destAddr is None:
                thesplog('Unexpected target inaccessibility for %s', msg,
                         level = logging.WARNING)
                raise CannotPickleAddress(destAddr)

            if msg is SendStatus.DeadTarget:
                thesplog('Faking message "sent" because target is dead and recursion avoided.')
                continue

            if destAddr == self._myQAddress:
                if incomingHandler is None:
                    return ReceiveEnvelope(sendAddr, msg)
                if not incomingHandler(ReceiveEnvelope(sendAddr, msg)):
                    return  # handler returned False, indicating run() should exit
            else:
                # Note: the following code has implicit knowledge of serialize() and xmit
                putQValue = lambda relayer: (relayer, (sendAddr, destAddr, msg))
                # Must forward this packet via a known forwarder or our parent.
                if destAddr in self._queues:
                    self._queues[destAddr].put(putQValue(self.myAddress), True)
                elif destAddr in self._fwdvia:
                    self._queues[self._fwdvia[destAddr]].put(putQValue(None))
                else:
                    # Not sure how to route this message yet.  It
                    # could be a heretofore silent child of one of our
                    # children, it could be our parent (whose address
                    # we don't know), or it could be elsewhere in the
                    # tree.
                    #
                    # Try sending it to the parent first.  If the
                    # parent can't determine the routing, it will be
                    # sent back down (relayAddr will be None in that
                    # case) and it must be sprayed out to all children
                    # in case the target lives somewhere beneath us.
                    # Note that _parentQ will be None for top-level
                    # actors, which send up to the Admin instead.
                    #
                    # As a special case, the external system is the
                    # parent of the admin, but the admin is the
                    # penultimate parent of all others, so this code
                    # must keep the admin and the parent from playing
                    # ping-pong with the message.  But... the message
                    # might be directed to the external system, which
                    # is the parent of the Admin, so we need to check
                    # with it first.
                    #   parentQ == None but adminQ good --> external
                    #   parentQ and adminQ and myAddress == adminAddr --> Admin
                    #   parentQ and adminQ and myAddress != adminADdr --> other Actor

                    if relayAddr:
                        # Send message up to the parent to see if the
                        # parent knows how to forward it
                        (self._parentQ or self._adminQ).put(putQValue(self.myAddress if self._parentQ else None), True)
                    else:
                        # Sent by parent or we are an external, so this
                        # may be some grandchild not currently known.
                        # Do the worst case and just send this message
                        # to ALL immediate children, hoping it will
                        # get there via some path.
                        for each in self._queues:
                            if each not in [self._adminAddr, str(self._adminAddr)]:
                                # None means sent by parent, so don't send BACK to parent if unknown
                                self._queues[each].put(putQValue(None), True)
        return None


    def abort_run(self, drain=False):
        # Queue transmits immediately, so no draining needed
        self._aborting_run = True


    def serializer(self, intent):
        wrappedMsg = self._myQAddress, intent.targetAddr, intent.message
        # For multiprocess Queues, the serialization (pickling) of the
        # outbound message happens in a separate process.  This is
        # unfortunate because if the message is not pickle-able, the
        # exception is thrown (and not handled) in the other process,
        # and this process has no indication of the issue.  The
        # unfortunate solution is that pickling must be tried in the
        # current process first to detect these errors (unfortunate
        # because that means each message gets pickled twice,
        # impacting performance).
        discard = pickle.dumps(wrappedMsg)
        return wrappedMsg

    def _scheduleTransmitActual(self, transmitIntent):
        if transmitIntent.targetAddr == self.myAddress:
            self._myInputQ.put( (self._myQAddress, transmitIntent.serMsg), True)
        elif transmitIntent.targetAddr in self._queues:
            self._queues[transmitIntent.targetAddr].put((self._myQAddress, transmitIntent.serMsg), True)
        else:
            # None means sent by parent, so don't send BACK to parent if unknown
            topOrFromBelow = self._myQAddress if self._parentQ else None
            (self._parentQ or self._adminQ).put((topOrFromBelow, transmitIntent.serMsg), True)

        transmitIntent.result = SendStatus.Sent
        transmitIntent.completionCallback()
