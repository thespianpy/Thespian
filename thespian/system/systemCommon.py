"Common elements for all Actors and Admin elements, regardless of implementation"

import logging
from thespian.actors import *
from thespian.system.utilis import thesplog, StatsManager
from thespian.system.ratelimit import RateThrottle
from thespian.system.addressManager import ActorAddressManager, CannotPickleAddress
from thespian.system.messages.logcontrol import SetLogging
from thespian.system.transport import *

# Assume a 2 KiB average packet size and a 100 Mb/s (12.5 MiB/s)
# network link, the link could be saturated by transmitting N
# packets/second (link_capacity / pktsize).  Current target is 70%
# saturation (arbitrarily).

RATE_THROTTLE = (lambda sizeAvg, linkSpeed, percentage:
                 int((linkSpeed / sizeAvg) * (percentage / 100.0)))(2048, 12.5*1024*1024, 70)


class systemCommonBase(object):

    def __init__(self, adminAddr, transport):
        self._adminAddr   = adminAddr
        self.transport    = transport
        self._addrManager = ActorAddressManager(adminAddr, self.transport.myAddress)
        self.transport.setAddressManager(self._addrManager)
        self._finalTransmitPending = {} # key = target ActorAddress, value=None or the last pending Intent
        self._awaitingAddressUpdate = {}  # key = actorAddress waited on (usually local), value=array of transmit Intents
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
        self._addrManager.deadAddress(childAddress)
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
        for addr in self._awaitingAddressUpdate:
            resp.addTXPendingAddressCount(addr, len(self._awaitingAddressUpdate[addr]))
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
        finalIntent = self._finalTransmitPending.get(
            intent.targetAddr,
            self._finalTransmitPending.get(sendAddr, None))
        self._finalTransmitPending[sendAddr or intent.targetAddr] = intent
        if finalIntent:
            finalIntent.nextIntent = intent
            self._sCBStats.inc('Actor.Message Send.Added to End of Sends')
            return
        self._send_intent_to_transport(intent)


    def _retryPendingChildOperations(self, childInstance, actualAddress):
        # actualAddress will be none if the child could not be created
        lcladdr = self._addrManager.getLocalAddress(childInstance)

        if not actualAddress:
            self._receiveQueue.append(ReceiveEnvelope(lcladdr, ChildActorExited(lcladdr)))

        if lcladdr in self._finalTransmitPending:
            # KWQ: what to do when actualAddress is None?
            self._finalTransmitPending[actualAddress] = self._finalTransmitPending[lcladdr]
            del self._finalTransmitPending[lcladdr]

        if lcladdr in self._awaitingAddressUpdate:
            pending = self._awaitingAddressUpdate[lcladdr]
            del self._awaitingAddressUpdate[lcladdr]
            for each in pending:
                if actualAddress:
                    # KWQ: confirm the following two lines can be removed; send_intent_to_transport should do this translation on its own.  At that point, the changeTargetAddr method should be able to be removed.
    #                if each.targetAddr == lcladdr:
    #                    each.changeTargetAddr(actualAddress)
                    # The only way this intent would have been on an
                    # _awaitingAddressUpdate list is if a previous attempt
                    # to actually transmit it it had already called
                    # _send_intent_to_transport; preservation of transmit
                    # ordering dictates it's still the right intent to
                    # perform:
                    self._sCBStats.inc('Actor.Message Send.Transmit ReInitiated')
                    self._send_intent_to_transport(each)
                else:
                    self._receiveQueue.append(
                        ReceiveEnvelope(self.myAddress,
                                        PoisonMessage(each.message)))
                    self._sCBStats.inc('Actor.Message Send.Poison Return on Child Abort')
                    each.result = SendStatus.Failed
                    each.completionCallback()


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
            thesplog('CannotPickleAddress, appending intent for %s (hash=%s)',
                     ex.address, hash(ex.address), level=logging.DEBUG)
            self._awaitingAddressUpdate.setdefault(ex.address, []).append(intent)
            self._sCBStats.inc('Actor.Message Send.Postponed for Address')
        except Exception:
            import traceback
            thesplog('Declaring transmit of %s as Poison: %s', intent.identify(),
                     traceback.format_exc(), exc_info=True, level=logging.ERROR)
            self._receiveQueue.append(ReceiveEnvelope(intent.targetAddr, PoisonMessage(intent.message)))
            self._sCBStats.inc('Actor.Message Send.Transmit Poison Rejection')
            intent.result = SendStatus.Failed
            intent.completionCallback()


    def _checkNextTransmit(self, result, completedIntent):
        # This is the callback for (all) TransmitIntents that will
        # send the next queued intent for that destination.
        if completedIntent.nextIntent:
            self._send_intent_to_transport(completedIntent.nextIntent)
        else:
            fkey = completedIntent.targetAddr
            if fkey not in self._finalTransmitPending:
                fkey = self._addrManager.sendToAddress(completedIntent.targetAddr)
                if fkey not in self._finalTransmitPending:
                    if isinstance(completedIntent.message, DeadEnvelope):
                        fkey = completedIntent.message.deadAddress
                        if fkey not in self._finalTransmitPending:
                            fkey = self._addrManager.sendToAddress(fkey)

            if fkey in self._finalTransmitPending:
                if self._finalTransmitPending[fkey] != completedIntent:
                    thesplog('Completed final intent %s does not match recorded final intent: %s',
                             completedIntent.identify(),
                             self._finalTransmitPending[fkey].identify(),
                             level=logging.WARNING)
                del self._finalTransmitPending[fkey]
            else:
                thesplog('Completed Transmit Intent %s for unrecorded destination %s / %s in %s',
                         completedIntent.identify(),
                         str(self._addrManager.sendToAddress(completedIntent.targetAddr)),
                         fkey,
                         str(map(str,self._finalTransmitPending.keys())),
                         level=logging.WARNING)
                self._sCBStats.inc('Action.Message Send.Unknown Completion')
                return

