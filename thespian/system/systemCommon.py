"Common elements for all Actors and Admin elements, regardless of implementation"

import logging
from thespian.actors import *
from thespian.system.utilis import thesplog, StatsManager
from thespian.system.ratelimit import RateThrottle
from thespian.system.addressManager import ActorAddressManager, CannotPickleAddress
from thespian.system.messages.logcontrol import SetLogging
from thespian.system.transport import *
from thespian.system.utilis import fmap
from itertools import chain
import traceback

# Assume a 2 KiB average packet size and a 100 Mb/s (12.5 MiB/s)
# network link, the link could be saturated by transmitting N
# packets/second (link_capacity / pktsize).  Current target is 70%
# saturation (arbitrarily).

RATE_THROTTLE = (lambda sizeAvg, linkSpeed, percentage:
                 int((linkSpeed / sizeAvg) * (percentage / 100.0)))(2048, 12.5*1024*1024, 70)


class PendingTransmits(object):
    def __init__(self):
        self._ftp = []

    def set_last_intent(self, stats, tgtAddr, sendAddr, intent):
        newval = (sendAddr or tgtAddr), intent
        for idx,each in enumerate(self._ftp):
            if each[0] == tgtAddr or each[0] == sendAddr:
                stats.inc('Actor.Message Send.Added to End of Sends')
                each[1].nextIntent = intent
                self._ftp[idx] = newval
                return False
        self._ftp.append(newval)
        return True

    def change_address_for_last(self, oldaddr, newaddr):
        for idx,each in enumerate(self._ftp):
            if each[0] == oldaddr:
                self._ftp[idx] = newaddr, each[1]
                return

    def last_intent_sent(self, addrmgr, intent):
        addrs = list(filter(None,
                            [intent.targetAddr,
                             getattr(intent.message, 'deadAddress', None), # DeadEnvelope
                            ]))
        fulladdrs = addrs + list(filter(None, map(addrmgr.sendToAddress, addrs)))
        for idx,each in enumerate(self._ftp):
            if each[0] in fulladdrs:
                if each[1] != intent:
                    thesplog('Completed final intent %s does not match recorded final intent: %s',
                             intent.identify(), each[1].identify(),
                             level=logging.WARNING)
                del self._ftp[idx]
                return True
        thesplog('Completed Transmit Intent %s for unrecorded destination %s / %s in %s',
                 intent.identify(),
                 str(intent.targetAddr),  # cannot addrManager.sendToAddress translate this address here.
                 addrs,
                 str(list(map(str,[F[0] for F in self._ftp]))),
                 level=logging.WARNING)
        return False


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
        self._finalTransmitPending = PendingTransmits()
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
        return True


    def _updateStatusResponse(self, resp):
        "Called to update a Thespian_SystemStatus or Thespian_ActorStatus with common information"
        for each in self.childAddresses:
            resp.addChild(each)
        for each in self._receiveQueue:
            resp.addReceivedMessage(each.sender, self.myAddress, each.message)
        self._sCBStats.copyToStatusResponse(resp)
        # Need to show _finalTransmitPending?  where is head of chain? shown by transport? (no)
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

    def _send_intent(self, intent):
        self._governer.eventRatePause()
        # Check if there are any existing transmits in progress to
        # this address (on either the input address or the validated
        # address); if so, just add the new one to the list and
        # return.
        sendAddr = self._addrManager.sendToAddress(intent.targetAddr)
        if self._finalTransmitPending.set_last_intent(
                self._sCBStats,
                intent.targetAddr,
                sendAddr,
                intent):
            self._send_intent_to_transport(intent)


    def _retryPendingChildOperations(self, childInstance, actualAddress):
        # actualAddress will be none if the child could not be created
        lcladdr = self._addrManager.getLocalAddress(childInstance)

        if not actualAddress:
            self._receiveQueue.append(ReceiveEnvelope(lcladdr, ChildActorExited(lcladdr)))

        self._finalTransmitPending.change_address_for_last(lcladdr, actualAddress)
        # KWQ: what to do when actualAddress is None?

        for each in self._awaitingAddressUpdate.remove_intents_for_address(lcladdr):
            if actualAddress:
                # KWQ: confirm the following two lines can be removed; send_intent_to_transport should do this translation on its own.  At that point, the changeTargetAddr method should be able to be removed.
#                if each.targetAddr == lcladdr:
#                    each.changeTargetAddr(actualAddress)
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
        try:
            self.transport.scheduleTransmit(self._addrManager, intent)
            self._sCBStats.inc('Actor.Message Send.Transmit Started')
        except CannotPickleAddress as ex:
            thesplog('CannotPickleAddress, appending intent for %s',
                     ex.address, level=logging.DEBUG)
            self._awaitingAddressUpdate.add(ex.address, intent)
            self._sCBStats.inc('Actor.Message Send.Postponed for Address')
            self._checkNextTransmit(0, intent)
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
        if completedIntent.nextIntent:
            self._send_intent_to_transport(completedIntent.nextIntent)
        else:
            if not self._finalTransmitPending.last_intent_sent(self._addrManager,
                                                               completedIntent):
                self._sCBStats.inc('Action.Message Send.Unknown Completion')

