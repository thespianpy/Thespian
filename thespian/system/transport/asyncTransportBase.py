"""This module provides a base class for transports to children that
are asynchronously created.  It handles issues related to
ActorLocalAddress resolution."""


from thespian.system.transport import TransmitOnly, SendStatus
from thespian.system.utilis import thesplog
import logging
from thespian.system.addressManager import ActorLocalAddress, CannotPickleAddress
try:
    # python 3 module name
    from queue import Queue, Empty
except ImportError:
    # python 2 module name
    from Queue import Queue, Empty
import threading

if hasattr(threading, 'main_thread'):
    # python 3.4 or later
    is_main_thread = lambda: threading.main_thread() == threading.current_thread()
else:
    if hasattr(threading, 'name'):
        is_main_thread = lambda: 'MainThread' in threading.current_thread().name
    else:
        is_main_thread = lambda: 'MainThread' in threading.current_thread().getName()


MAX_PENDING_TRANSMITS = 20
MAX_QUEUED_TRANSMITS  = 950
QUEUE_TRANSMIT_UNBLOCK_THRESHOLD = 780


class asyncTransportBase(object):
    """This class should be used as a base-class for Transports where the
       transmit operation occurs asynchronously.  The send operation
       will reject TransmitIntent objects until they are fully
       serializeable, and will then submit the TransmitIntent to the
       actual Transport for sending.

       This module provides queue management for transmits to ensure
       that only a limited number of transmits are active from this
       Actor at any one time.  Note that the system level
       functionality is responsible for ensuring that only one
       TransmitIntent *PER TARGET* is submitted to this module at any
       one time, but this module ensures that the number of
       TransmitIntents *FOR ALL TARGETS* does not exceed a maximum
       threshold.
    """


    # Expects from subclass:
    #   self.serializer         - serializer callable that returns serialized form
    #                             of intent that should be sent (stored in .serMsg)
    #   self._scheduleTransmitActual -- called to do the actual transmit (with .serMsg set)

    def __init__(self, *args, **kw):
        super(asyncTransportBase, self).__init__(*args, **kw)
        self._aTB_numPendingTransmits = 0
        self._aTB_queuedPendingTransmits = Queue()
        self._aTB_submitting = []

    def setAddressManager(self, addrManager):
        self._addressMgr = addrManager


    def _updateStatusResponse(self, resp):
        "Called to update a Thespian_SystemStatus or Thespian_ActorStatus with common information"
        for each in range(self._aTB_queuedPendingTransmits.qsize()):
            #n.b. cannot safely walk queue to get info on each entry, so return placeholders
            resp.addPendingMessage(self.myAddress, '<targetAddr>', '<message>')


    def _canSendNow(self, intent):
        return getattr(intent, "can_send_now", False) or \
            (MAX_PENDING_TRANSMITS > self._aTB_numPendingTransmits and
             is_main_thread() and
             self._aTB_queuedPendingTransmits.empty())

    def _runQueued(self, _TXresult, _TXIntent):
        self._aTB_numPendingTransmits -= 1
        try:
            nextTransmit = self._aTB_queuedPendingTransmits.get_nowait()
        except (Empty, IndexError):
            return  # no more pending
        self._submitTransmit(nextTransmit)


    def scheduleTransmit(self, addressManager, transmitIntent):

        """Requests that a transmit be performed.  The message and target
           address must be fully valid at this point; any local
           addresses should throw a CannotPickleAddress exception and
           the caller is responsible for retrying later when those
           addresses are available.

           If addressManager is None then the intent address is
           assumed to be valid but it cannot be updated if it is a
           local address or a dead address.  A value of None is
           normally only used at Admin or Actor startup time when
           confirming the established connection back to the parent,
           at which time the target address should always be valid.
        """

        if addressManager:
            # Verify the target address is useable
            targetAddr, txmsg = addressManager.prepMessageSend(transmitIntent.targetAddr,
                                                               transmitIntent.message)
            try:
                isDead = txmsg == SendStatus.DeadTarget
            except Exception:
                # txmsg may have an __eq__ that threw an exception on comparison
                isDead = False
            if isDead:
                # Address Manager has indicated that these messages
                # should never be attempted because the target is
                # dead.  This is *only* for special messages like
                # DeadEnvelope and ChildActorExited which would
                # endlessly recurse or bounce back and forth.  This
                # code indicates here that the transmit was
                # "successful" to allow normal cleanup but to avoid
                # recursive error generation.
                thesplog('Faking transmit result Sent for %s because target is dead',
                         transmitIntent, level = logging.WARNING)
                transmitIntent.result = SendStatus.Sent
                transmitIntent.completionCallback()
                return

            if not targetAddr:
                raise CannotPickleAddress(transmitIntent.targetAddr)

            # In case the prep made some changes...
            transmitIntent.changeTargetAddr(targetAddr)
            transmitIntent.changeMessage(txmsg)

        # Verify that the message can be serialized.  This may throw
        # an exception, which will cause the caller to store this
        # intent and retry it at some future point (the code up to and
        # including this serialization should be idempotent).

        transmitIntent.serMsg = self.serializer(transmitIntent)
        self._schedulePreparedIntent(transmitIntent)


    def _schedulePreparedIntent(self, transmitIntent):
        # If there's nothing to send, that's implicit success
        if not transmitIntent.serMsg:
            transmitIntent.result = SendStatus.Sent
            transmitIntent.completionCallback()
            return

        # OK, this can be sent now, so go ahead and get it sent out
        if not self._canSendNow(transmitIntent):
            self._aTB_queuedPendingTransmits.put(transmitIntent)
            if self._aTB_queuedPendingTransmits.qsize() >= MAX_QUEUED_TRANSMITS:
                # Try to drain out local work before accepting more
                # because it looks like we're getting really behind.
                # This is dangerous though, because if other Actors
                # are having the same issue this can create a
                # deadlock.
                thesplog('Entering tx-only mode to drain excessive queue (%s > %s, drain-to %s)',
                         self._aTB_queuedPendingTransmits.qsize(),
                         MAX_QUEUED_TRANSMITS,
                         QUEUE_TRANSMIT_UNBLOCK_THRESHOLD,
                         level = logging.WARNING)
                while self._aTB_queuedPendingTransmits.qsize() > QUEUE_TRANSMIT_UNBLOCK_THRESHOLD:
                    self.run(TransmitOnly, transmitIntent.delay())
                thesplog('Exited tx-only mode after draining excessive queue (%s)',
                         self._aTB_queuedPendingTransmits.qsize(),
                         level = logging.WARNING)
            return

        self._submitTransmit(transmitIntent)


    def _submitTransmit(self, transmitIntent):
        self._aTB_numPendingTransmits += 1
        transmitIntent.addCallback(self._runQueued, self._runQueued)

        if self._aTB_submitting:
            self._aTB_submitting.insert(0, transmitIntent)  # recursion protection
            return

        self._aTB_submitting = [transmitIntent]

        while self._aTB_submitting:
            tx = self._aTB_submitting[-1]
            try:
                thesplog('actualTransmit of %s', tx.identify(), level=logging.DEBUG)
                self._scheduleTransmitActual(tx)
            finally:
                self._aTB_submitting.pop()


    def deadAddress(self, addressManager, childAddr):
        # Go through pending transmits and update any to this child to a dead letter delivery
        oldQ = self._aTB_queuedPendingTransmits
        # n.b. This rebuilds the Queue; the entries from the original
        # queue are preserved in the same order, but it is possible
        # that concurrent transmit attempts from other threads will be
        # interspersed in the new Queue.
        self._aTB_queuedPendingTransmits = Queue()
        while True:
            try:
                each = oldQ.get_nowait()
            except Empty:
                break
            if each.targetAddr == childAddr:
                newtgt, newmsg = addressManager.prepMessageSend(each.targetAddr, each.message)
                each.changeTargetAddr(newtgt)
                # n.b. prepMessageSend might return
                # SendStatus.DeadTarget for newmsg; when this is later
                # attempted, that will be handled normally and the
                # transmit will be completed as "Sent"
                each.changeMessage(newmsg)
            self._aTB_queuedPendingTransmits.put(each)


