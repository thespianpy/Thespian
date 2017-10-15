"Common elements for all Actors and Admin elements, regardless of implementation"

import logging
from thespian.actors import *
from thespian.system.utilis import thesplog, StatsManager, AssocList
from thespian.system.timing import ExpirationTimer, unexpired
from thespian.system.ratelimit import RateThrottle
from thespian.system.addressManager import (ActorAddressManager,
                                            CannotPickleAddress,
                                            ActorLocalAddress)
from thespian.system.messages.logcontrol import SetLogging
from thespian.system.transport import *
from thespian.system.utilis import fmap
from itertools import chain
from datetime import datetime
import traceback


MAX_SHUTDOWN_DRAIN_PERIOD=timedelta(seconds=7)

# Assume a 2 KiB average packet size and a 100 Mb/s (12.5 MiB/s)
# network link, the link could be saturated by transmitting N
# packets/second (link_capacity / pktsize).  Current target is 70%
# saturation (arbitrarily).

RATE_THROTTLE = (lambda sizeAvg, linkSpeed, percentage:
                 int((linkSpeed / sizeAvg) * (percentage / 100.0)))(2048, 12.5*1024*1024, 70)


class PendingTransmits(object):
    def __init__(self, address_manager):
        self._addrmgr = address_manager
        # There are expected to be a low number of pending transmits
        # for a typical actor.  At present time, this is an array of
        # intents.  Note that an intent may fail due to a
        # CannotPickleAddress (which calls cannot_send_now() ), and
        # then later be passed here again to can_send_now() when the
        # address becomes known.
        self._atd = AssocList()  # address -> ptl index
        self._ptl = []

    def _intent_addresses(self, intent):
        yield intent.targetAddr
        xlated_addr = self._addrmgr.sendToAddress(intent.targetAddr)
        if xlated_addr:
            yield xlated_addr
        deadaddr = getattr(intent.message, 'deadAddress', None)
        if deadaddr:
            yield deadaddr
            xlated_addr = self._addrmgr.sendToAddress(deadaddr)
            if xlated_addr:
                yield xlated_addr

    def p_can_send_now(self, stats, intent):
        addrs = list(self._intent_addresses(intent))
        for idx, addr in enumerate(addrs):
            ptloc = self._atd.find(addr)
            if ptloc is not None:
                for ii in range(idx):
                    self._atd.add(addrs[ii], ptloc)
                break
        else:
            ptloc = len(self._ptl)
            self._ptl.append([])
            for addr in addrs:
                self._atd.add(addr, ptloc)
        if not(self._ptl[ptloc]):
            self._ptl[ptloc] = [intent]
            return True
        self._ptl[ptloc].append(intent)
        return False

    def get_next(self, completed_intent):
        for addr in self._intent_addresses(completed_intent):
            ptloc = self._atd.find(addr)
            if ptloc is not None:
                break
        else:
            thesplog('No pending transmits for completed intent: %s',
                     completed_intent, level=logging.ERROR)
            return None
        if not(self._ptl[ptloc]):
            thesplog('No pending entry for completed intent: %s',
                     completed_intent, level=logging.ERROR)
            return None
        self._ptl[ptloc].pop(0)
        if self._ptl[ptloc]:
            return self._ptl[ptloc][0]

        # Trim here...
        return None

    def cannot_send_now(self, intent):
        return self.get_next(intent)

    def change_address_for_transmit(self, oldaddr, newaddr):
        oldidx = self._atd.find(oldaddr)
        if oldidx is None:
            # Have not scheduled any transmits for this (probably new)
            # child yet.
            return
        newidx = self._atd.find(newaddr)
        if newidx is None:
            self._atd.add(newaddr, oldidx)
        elif newidx != oldidx:
            if isinstance(oldaddr.addressDetails, ActorLocalAddress):
                # This can happen if sends are made to createActor
                # results with a globalName before the actual address
                # is known.  Each createActor creates a local address,
                # but all those local addresses map back to the same
                # actual address.
                self._ptl[newidx].extend(self._ptl[oldidx])
                self._atd.add(oldaddr, newidx)
                self._ptl[oldidx] = []  # should not be used anymore
            else:
                thesplog('Duplicate pending transmit indices'
                         ': %s -> %s, %s -> %s',
                         oldaddr, oldidx, newaddr, newidx,
                         level=logging.ERROR)

    def update_status_response(self, stats, my_address):
        for group in self._ptl:
            for each in group:
                stats.addPendingMessage(my_address, each.targetAddr,
                                        each.message)


class AddressWaitTransmits(object):
    def __init__(self):
        self._awt = []
        # key = actorAddress waited on (usually local), value=array of transmit Intents
    def fmap(self, func): map(func, self._awt)
    def add(self, addr, intent):
        for each in self._awt:
            if each[0] == addr:
                each[1].append(intent)
                return
        self._awt.append( (addr, [intent]) )
    def remove_intents_for_address(self, addr):
        for idx, each in enumerate(self._awt):
            if each[0] == addr:
                del self._awt[idx]
                return each[1]
        return []

len_second = lambda x: (x[0], len(x[1]))



class systemCommonBase(object):

    def __init__(self, adminAddr, transport):
        self._adminAddr   = adminAddr
        self.transport    = transport
        self._addrManager = ActorAddressManager(adminAddr, self.transport.myAddress)
        self.transport.setAddressManager(self._addrManager)
        self._pendingTransmits = PendingTransmits(self._addrManager)
        self._awaitingAddressUpdate = AddressWaitTransmits()
        self._receiveQueue = []  # array of ReceiveMessage to be processed
        self._children = []  # array of Addresses of children of this Actor/Admin
        self._governer = RateThrottle(RATE_THROTTLE)
        self._sCBStats = StatsManager()


    @property
    def address(self): return self.transport.myAddress
    @property
    def myAddress(self): return self.transport.myAddress


    @property
    def childAddresses(self): return self._children

    def _registerChild(self, childAddress): self._children.append(childAddress)

    def _handleChildExited(self, childAddress):
        self._sCBStats.inc('Common.Message Received.Child Actor Exited')
        self.transport.deadAddress(self._addrManager, childAddress)
        self._childExited(childAddress)
        self._children = [C for C in self._children if C != childAddress]
        if hasattr(self, '_exiting') and not self._children:
            # OK, all children are dead, can now exit this actor, but
            # make sure this final cleanup only occurs once
            # (e.g. transport.deadAddress above could recurse through
            # here as well.
            if not hasattr(self, '_exitedAlready'):
                self._exitedAlready = True
                self._sayGoodbye()
                self.transport.abort_run(drain=True)
            return False
        return True


    def _updateStatusResponse(self, resp):
        "Called to update a Thespian_SystemStatus or Thespian_ActorStatus with common information"
        for each in self.childAddresses:
            resp.addChild(each)
        for each in self._receiveQueue:
            resp.addReceivedMessage(each.sender, self.myAddress, each.message)
        self._sCBStats.copyToStatusResponse(resp)
        self._pendingTransmits.update_status_response(resp, self.myAddress)
        resp.governer = str(self._governer)
        fmap(lambda x: resp.addTXPendingAddressCount(*len_second(x)),
             self._awaitingAddressUpdate)
        self.transport._updateStatusResponse(resp)


    def setLoggingControls(self, envelope):
        from thespian.system.utilis import thesplog_control
        msg = envelope.message
        thesplog_control(msg.threshold, msg.useLogging, msg.useFile)
        return True


    # ----------------------------------------------------------------------
    # Transmit management

    def _send_intent(self, intent, curtime=None):
        self._governer.eventRatePause(curtime or datetime.now())
        # Check if there are any existing transmits in progress to
        # this address (on either the input address or the validated
        # address); if so, just add the new one to the list and
        # return.
        if self._pendingTransmits.p_can_send_now(self._sCBStats, intent):
            self._send_intent_to_transport(intent)


    def _retryPendingChildOperations(self, childInstance, actualAddress):
        # actualAddress will be none if the child could not be created
        lcladdr = self._addrManager.getLocalAddress(childInstance)

        if not actualAddress:
            self._receiveQueue.append(ReceiveEnvelope(lcladdr,
                                                      ChildActorExited(lcladdr)))
        else:
            self._pendingTransmits.change_address_for_transmit(lcladdr,
                                                               actualAddress)

        for each in self._awaitingAddressUpdate\
                        .remove_intents_for_address(lcladdr):
            if actualAddress:
                self._sCBStats.inc('Actor.Message Send.Transmit ReInitiated')
                self._send_intent(each)
            else:
                if not isinstance(each.message, PoisonMessage):
                    self._receiveQueue.append(
                        ReceiveEnvelope(
                            self.myAddress,
                            PoisonMessage(each.message, 'Child Aborted')))
                self._sCBStats.inc('Actor.Message Send.Poison Return on Child Abort')
                each.tx_done(SendStatus.Failed)


    def _send_intent_to_transport(self, intent):
        thesplog('Attempting intent %s', intent.identify(), level=logging.DEBUG)
        if not hasattr(intent, '_addedCheckNextTransmitCB'):
            intent.addCallback(self._checkNextTransmit, self._checkNextTransmit)
            # Protection against duplicate callback additions in case
            # of a retry due to the CannotPickleAddress exception below.
            intent._addedCheckNextTransmitCB = True
        intent._transmit_pending_to_transport = True
        try:
            self.transport.scheduleTransmit(self._addrManager, intent)
            self._sCBStats.inc('Actor.Message Send.Transmit Started')
            return
        except CannotPickleAddress as ex:
            thesplog('CannotPickleAddress, appending intent for %s',
                     ex.address, level=logging.DEBUG)
            self._sCBStats.inc('Actor.Message Send.Postponed for Address')
            self._awaitingAddressUpdate.add(ex.address, intent)
            # Callback is still registered, so callback can use the
            # _transmit_pending_to_transport to determine if it was
            # actually being transmitted or not.
            intent._transmit_pending_to_transport = False
            next_intent = self._pendingTransmits.cannot_send_now(intent)
            if next_intent:
                self._send_intent_to_transport(next_intent)
        except Exception:
            import traceback
            thesplog('Declaring transmit of %s as Poison: %s', intent.identify(),
                     traceback.format_exc(), exc_info=True, level=logging.ERROR)
            if not isinstance(intent.message, PoisonMessage):
                self._receiveQueue.append(
                    ReceiveEnvelope(intent.targetAddr,
                                    PoisonMessage(intent.message,
                                                  traceback.format_exc())))
            self._sCBStats.inc('Actor.Message Send.Transmit Poison Rejection')
            intent.tx_done(SendStatus.Failed)



    def _checkNextTransmit(self, result, completedIntent):
        # This is the callback for (all) TransmitIntents that will
        # send the next queued intent for that destination.
        if getattr(completedIntent, '_transmit_pending_to_transport', False):
            next_intent = self._pendingTransmits.get_next(completedIntent)
            if next_intent:
                self._send_intent_to_transport(next_intent)


    def drainTransmits(self):
        drainLimit = ExpirationTimer(MAX_SHUTDOWN_DRAIN_PERIOD)
        for drain_remaining_time in unexpired(drainLimit):
            if not self.transport.run(TransmitOnly, drain_remaining_time.remaining()):
                break  # no transmits left
