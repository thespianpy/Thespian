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


class ActorStartupFailed(Exception):
    """This is an exception thrown when the actor's startup attempt and
       connection back to its parent has failed.  Because the actor
       did not complete startup operations, it cannot perform a
       graceful shutdown and so should exit immediately.
    """
    pass


def actorStartupFailed(*args, **kw):
    # Useable as a callback function
    raise ActorStartupFailed()


class systemCommonBase(object):

    def __init__(self, adminAddr, transport):
        self._adminAddr   = adminAddr
        self.transport    = transport
        self._addrManager = ActorAddressManager(adminAddr, self.transport.myAddress)
        self.transport.setAddressManager(self._addrManager)
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
        self._send_intent_to_transport(intent)


    def _retryPendingChildOperations(self, childInstance, actualAddress):
        # actualAddress will be None if the child could not be created
        lcladdr = self._addrManager.getLocalAddress(childInstance)

        if not actualAddress:
            self._receiveQueue.append(ReceiveEnvelope(lcladdr,
                                                      ChildActorExited(lcladdr)))

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
        # Set a flag on this intent indicating that it has been passed
        # down to the actual transport layer.  This is used in
        # completion callbacks to determine if the completion means
        # that the lower transport is now ready for more; the
        # alternative is intents that never reached the actual
        # transport layer lower down, and therefore did not consume
        # resources there.
        try:
            self.transport.scheduleTransmit(self._addrManager, intent)
            self._sCBStats.inc('Actor.Message Send.Transmit Started')
            return None  # no retry
        except CannotPickleAddress as ex:
            thesplog('CannotPickleAddress, appending intent for %s',
                     ex.address, level=logging.DEBUG)
            self._sCBStats.inc('Actor.Message Send.Postponed for Address')
            self._awaitingAddressUpdate.add(ex.address, intent)
            return None
        except Exception:
            import traceback
            thesplog('Declaring transmit of %s as Poison: %s', intent.identify(),
                     traceback.format_exc(), exc_info=True, level=logging.ERROR)
            if not isinstance(intent.message, logging.LogRecord):
                logging.error('Declaring transmit of %s as Poison: %s', intent.identify(),
                              traceback.format_exc(), exc_info=True)
            if not isinstance(intent.message, PoisonMessage):
                self._receiveQueue.append(
                    ReceiveEnvelope(intent.targetAddr,
                                    PoisonMessage(intent.message,
                                                  traceback.format_exc())))
            self._sCBStats.inc('Actor.Message Send.Transmit Poison Rejection')
            intent.tx_done(SendStatus.Failed)
            return None


    def drainTransmits(self):
        drainLimit = ExpirationTimer(MAX_SHUTDOWN_DRAIN_PERIOD)
        for drain_remaining_time in unexpired(drainLimit):
            if not self.transport.run(TransmitOnly, drain_remaining_time.remaining()):
                break  # no transmits left
