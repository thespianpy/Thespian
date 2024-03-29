"""Simple UDP sockets.

Each actor has a UDP IPv4 port / socket that it listens for incoming messages on.

Actors are capable of creating sub-actors and addressing them without
requiring intervention from the Agency.

There is no guarantee that messages have been delivered.  Send
failures can be returned, but the lack of a send failure is no
guarantee that the message was actually delivered.

This transport can be used within a process, between processes, and
even between processes on separate systems.

"""


import logging
from thespian.system.utilis import thesplog
from thespian.actors import *
from thespian.system.transport import *
from thespian.system.transport.IPBase import *
from thespian.system.timing import ExpirationTimer, currentTime
from thespian.system.utilis import partition
from thespian.system.messages.convention import CONV_ADDR_IPV4_CAPABILITY
from thespian.system.messages.multiproc import ChildMayHaveDied
from thespian.system.addressManager import ActorLocalAddress
import socket
import select
from datetime import timedelta
try:
    import cPickle as pickle
except Exception:
    import pickle    # type: ignore
from thespian.system.transport.asyncTransportBase import asyncTransportBase
from thespian.system.transport.wakeupTransportBase import wakeupTransportBase
from thespian.system.transport.errmgmt import err_select_retry

DEFAULT_ADMIN_PORT = 1029

DEAD_ADDRESS_TIMEOUT = timedelta(seconds=15)
INTERRUPT_SUPPRESSION_TIME = timedelta(seconds=1)

serializer = pickle
# json cannot be used because Messages are often structures, which cannot be converted to JSON.


class UDPEndpoint(TransportInit__Base):  # internal use by this module only
    def __init__(self, *args): self.args = args
    @property
    def addrInst(self): return self.args[0]


class UDPTransportCopy(object): pass


class UDPTransport(asyncTransportBase, wakeupTransportBase):
    "A transport using UDP IPv4 sockets for communications."

    def __init__(self, initType, *args):
        super(UDPTransport, self).__init__()

        templateAddr = None
        if isinstance(initType, ExternalInterfaceTransportInit):
            # External process that is going to talk "in".  There is
            # no parent, and the child is the systemAdmin.
            capabilities, logDefs, concurrency_context = args
            self._adminAddr       = self.getAdminAddr(capabilities)
            self._parentAddr      = None
        elif isinstance(initType, UDPEndpoint):
            instanceNum, assignedAddr, self._parentAddr, self._adminAddr = initType.args
            templateAddr = assignedAddr
            # N.B.  myAddress is actually the address we will export
            # for others to talk to us, not the bind address.  The
            # difference is that we bind to '0.0.0.0' (inaddr_any),
            # but that's not a valid address for people to send stuff
            # to us.
        elif isinstance(initType, UDPTransportCopy):
            self._adminAddr = args[0]
            self._parentAddr = None
        else:
            thesplog('UDPTransport init of type %s unsupported', str(initType), level=logging.ERROR)
        if not templateAddr:
            templateAddr = ActorAddress(UDPv4ActorAddress(None, 0))
        self.socket = socket.socket(*templateAddr.addressDetails.socketArgs)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(*templateAddr.addressDetails.bindArgs)
        self.myAddress = ActorAddress(UDPv4ActorAddress(*self.socket.getsockname(),
                                                        external=True))
        self._rcvd = []
        self._checkChildren = False
        self._shutdownSignalled = False
        self._interruptWaitCounter = 0
        self._interruptWaitSilencer = None
        self._pending_actions = [] # array of (ExpirationTimer, func)


    def close(self):
        """Releases all resources and terminates functionality.  This is
           better done deterministically by explicitly calling this
           method (although __del__ will attempt to perform similar
           operations), but it has the unfortunate side-effect of
           making this object modal: after the close it can be
           referenced but not successfully used anymore, so it
           explicitly nullifies its contents.
        """
        if hasattr(self, '_pending_actions'):
            delattr(self, '_pending_actions')
        if hasattr(self, 'socket'):
            self.socket.close()
            delattr(self, 'socket')


    def external_transport_clone(self):
        # Return a unique context for actor communication from external
        return UDPTransport(UDPTransportCopy(), self._adminAddr)


    def protectedFileNumList(self):
        return [self.socket.fileno()]


    def childResetFileNumList(self):
        return self.protectedFileNumList()


    @staticmethod
    def getAdminAddr(capabilities):
        return ActorAddress(
            UDPv4ActorAddress(None,
                              capabilities.get('Admin Port', DEFAULT_ADMIN_PORT),
                              external=UDPTransport.getConventionAddress(capabilities) or True))

    @staticmethod
    def getAddressFromString(addrspec):
        if isinstance(addrspec, tuple):
            addrparts = addrspec
        else:
            addrparts = addrspec.split(':')
        return ActorAddress(UDPv4ActorAddress(addrparts[0], addrparts[1], external=True))

    @staticmethod
    def getConventionAddress(capabilities):
        convAddr = capabilities.get(CONV_ADDR_IPV4_CAPABILITY, None)
        if not convAddr:
            return None
        try:
            if isinstance(convAddr, list):
                return [UDPTransport.getAddressFromString(a) for a in convAddr]
            return [UDPTransport.getAddressFromString(convAddr)]
        except Exception as ex:
            thesplog('Invalid UCP convention address entry "%s": %s',
                     convAddr, ex,
                     level=logging.ERROR)
            raise InvalidActorAddress(convAddr, str(ex))


    def _updateStatusResponse(self, resp):
        "Called to update a Thespian_SystemStatus or Thespian_ActorStatus with common information"
        asyncTransportBase._updateStatusResponse(self, resp)
        wakeupTransportBase._updateStatusResponse(self, resp)


    @staticmethod
    def probeAdmin(addr):
        """Called to see if there might be an admin running already at the
           specified addr.  This is called from the systemBase, so
           simple blocking operations are fine.  This only needs to
           check for a responder; higher level logic will verify that
           it's actually an ActorAdmin suitable for use.
        """
        ss = socket.socket(*addr.addressDetails.socketArgs)
        try:
            ss.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
            try:
                ss.bind(*addr.addressDetails.bindArgs)
                # no other process bound
                return False
            except socket.error as ex:
                import errno
                if ex.errno == errno.EADDRINUSE:
                    return True
                # Some other error... not sure if that means an admin is running or not.
                return False  # assume not
        finally:
            ss.close()


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
            return UDPEndpoint(assignedLocalAddr.addressDetails.addressInstanceNum,
                               None,
                               self.myAddress,
                               self._adminAddr)
        return UDPEndpoint(None,
                           assignedLocalAddr,  # assumed to be an actual UDPActorAddress-based address (e.g. admin)
                           self.myAddress,
                           self._adminAddr)

    def connectEndpoint(self, endPoint):
        pass


    def deadAddress(self, addressManager, childAddr):
        # UDP is unable to indicate whether the target address is
        # still alive or not, so this entry point is unlikely to be
        # utilized.  In addition, UDP can use recycled ports, so the
        # hopeful workaround here is to mark the address as dead for a
        # period of time and hope that the port is not recycled by the
        # system in that time frame.
        addressManager.deadAddress(childAddr)
        self._pending_actions.append( (ExpirationTimer(DEAD_ADDRESS_TIMEOUT),
                                       lambda am=addressManager, addr=childAddr:
                                       addressManager.remove_dead_address(addr)))
        super(UDPTransport, self).deadAddress(addressManager, childAddr)


    def serializer(self, intent):
        return serializer.dumps(intent.message)


    def interrupt_wait(self,
                       signal_shutdown=False,
                       check_children=False):
        self._shutdownSignalled |= signal_shutdown
        self._checkChildren |= check_children
        if getattr(self, '_interruptWaitSilencer', None):
            if not self._interruptWaitSilencer.view().expired():
                return
            self._interruptWaitSilencer = None
            self._interruptWaitCounter = 0
        else:
            if hasattr(self, '_interruptWaitCounter'):
                if self._interruptWaitCounter > 10:
                    self._interruptWaitSilencer = ExpirationTimer(INTERRUPT_SUPPRESSION_TIME)
                    return
                self._interruptWaitCounter += 1
            else:
                self._interruptWaitCounter = 0
        if self._shutdownSignalled or self._checkChildren:
            # Under some python implementations, signal handling (which
            # could generate an ActorShutdownRequest) can be performed
            # without interrupting the underlying syscall, so this message
            # is otherwise ignored but causes the select.select below to
            # return.
            self.socket.sendto(b'BuMP', self.myAddress.addressDetails.sockname)


    def _scheduleTransmitActual(self, transmitIntent, has_exclusive_flag=False):
        if transmitIntent.targetAddr == self.myAddress:
            self._rcvd.append(ReceiveEnvelope(transmitIntent.targetAddr,
                                              transmitIntent.message))
            self.interrupt_wait()
            r = True
        else:
            # UDPTransport transmit is serially blocking, but both sender
            # and receiver provide lots of buffering.  At present, there
            # is no receipt confirmation (KWQ: but there should be)
            r = self.socket.sendto(transmitIntent.serMsg,
                                   transmitIntent.targetAddr.addressDetails.sockname)
        transmitIntent.tx_done(SendStatus.Sent
                               if r else
                               SendStatus.BadPacket)


    def _runWithExpiry(self, incomingHandler):
        if incomingHandler == TransmitOnly or \
           isinstance(incomingHandler, TransmitOnly):
            # transmits are not queued/multistage in this transport, no waiting
            return 0

        self._aborting_run = None

        while self._aborting_run is None:
            ct = currentTime()
            if self.run_time.view(ct).expired():
                break
            if self._checkChildren:
                self._checkChildren = False
                rcvdEnv = ReceiveEnvelope(self.myAddress, ChildMayHaveDied())
            elif self._shutdownSignalled:
                self._shutdownSignalled = False
                rcvdEnv = ReceiveEnvelope(self.myAddress, ActorExitRequest())
            elif self._rcvd:
                rcvdEnv = self._rcvd.pop()
            else:
                next_action_timeout = self.check_pending_actions(ct)
                try:
                    sresp, _ign1, _ign2 = select.select([self.socket.fileno()], [], [],
                                                        min(self.run_time, next_action_timeout)
                                                        .view(ct).remainingSeconds())
                except (OSError, select.error) as se:
                    errnum = getattr(se, 'errno', se.args[0])
                    if err_select_retry(errnum):
                        thesplog('UDP select retry on %s', se, level=logging.DEBUG)
                        continue
                    thesplog('Error during UDP select: %s', se, level=logging.CRITICAL)
                    return Thespian__Run_Errored(se)
                except ValueError:
                    # self.run_time can expire between the while test
                    # and the use in the select statement.
                    continue

                if [] == sresp:
                    if [] == _ign1 and [] == _ign2:
                        # Timeout, give up
                        return Thespian__Run_Expired()
                    thesplog('Waiting for read event, but got %s %s', _ign1, _ign2, level=logging.WARNING)
                    continue
                rawmsg, sender = self.socket.recvfrom(65535)
                if rawmsg == b'BuMP':
                    if self._checkChildren or self._shutdownSignalled:
                        continue
                    return Thespian__UpdateWork()
                else:
                    sendAddr = ActorAddress(UDPv4ActorAddress(*sender, external=True))
                    try:
                        msg = serializer.loads(rawmsg)
                    except Exception:
                        continue
                rcvdEnv = ReceiveEnvelope(sendAddr, msg)
            if incomingHandler is None:
                return rcvdEnv
            r = Thespian__Run_HandlerResult(incomingHandler(rcvdEnv))
            if not r:
                # handler returned false-ish, indicating run() should exit
                return r

        if self._aborting_run is not None:
            return self._aborting_run

        return Thespian__Run_Expired()


    def check_pending_actions(self, current_time):
        expired, rem = partition(lambda E: E[0].view(current_time).expired(),
                                 self._pending_actions)
        for each in expired:
            each[1]()
        self._pending_actions = rem
        return min([E[0] for E in self._pending_actions] + [ExpirationTimer(None)])

    def abort_run(self, drain=False):
        """Indicates that run should exit; similar to a handler returning
           False except this can be called from anywhere.  If
           drain=True, then the run() will wait (a reasonable amount
           of time) for system-related messages to be transmitted
           before returning, otherwise run() will terminate as soon as
           control returns to it from this call.
        """
        # UDPTransport does not queue transmits but handles them
        # inline, so no draining required.
        self._aborting_run = Thespian__Run_Terminated()
