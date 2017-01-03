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
from thespian.system.timing import ExpirationTimer
from thespian.system.utilis import partition
from thespian.system.messages.multiproc import ChildMayHaveDied
from thespian.system.addressManager import ActorLocalAddress
import socket
import select
from datetime import timedelta
import pickle
from thespian.system.transport.asyncTransportBase import asyncTransportBase
from thespian.system.transport.wakeupTransportBase import wakeupTransportBase


DEAD_ADDRESS_TIMEOUT = timedelta(seconds=15)


serializer = pickle
# json cannot be used because Messages are often structures, which cannot be converted to JSON.


class UDPEndpoint(TransportInit__Base):  # internal use by this module only
    def __init__(self, *args): self.args = args
    @property
    def addrInst(self): return self.args[0]


class UDPTransport(asyncTransportBase, wakeupTransportBase):
    "A transport using UDP IPv4 sockets for communications."

    def __init__(self, initType, *args):
        super(UDPTransport, self).__init__()

        if isinstance(initType, ExternalInterfaceTransportInit):
            # External process that is going to talk "in".  There is
            # no parent, and the child is the systemAdmin.
            capabilities, logDefs, concurrency_context = args
            templateAddr          = UDPv4ActorAddress(None, 0)
            self.socket           = socket.socket(*templateAddr.socketArgs)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind(*templateAddr.bindArgs)
            self.myAddress        = ActorAddress(UDPv4ActorAddress(*self.socket.getsockname(),
                                                                   external=True))
            thesplog('external template %s got actual %s', templateAddr, self.myAddress,
                     level=logging.DEBUG)
            self._adminAddr       = self.getAdminAddr(capabilities)
            self._parentAddr      = None
        elif isinstance(initType, UDPEndpoint):
            instanceNum, assignedAddr, self._parentAddr, self._adminAddr = initType.args
            templateAddr = assignedAddr or ActorAddress(UDPv4ActorAddress(None, 0))
            self.socket           = socket.socket(*templateAddr.addressDetails.socketArgs)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind(*templateAddr.addressDetails.bindArgs)
            # N.B.  myAddress is actually the address we will export
            # for others to talk to us, not the bind address.  The
            # difference is that we bind to '0.0.0.0' (inaddr_any),
            # but that's not a valid address for people to send stuff
            # to us.
            self.myAddress = ActorAddress(UDPv4ActorAddress(*self.socket.getsockname(),
                                                            external=True))
        else:
            thesplog('UDPTransport init of type %s unsupported', str(initType), level=logging.ERROR)
        self._rcvd = []
        self._checkChildren = False
        self._shutdownSignalled = False
        self._pending_actions = [] # array of (ExpirationTimer, func)

    def protectedFileNumList(self):
        return [self.socket.fileno()]

    def childResetFileNumList(self):
        return self.protectedFileNumList()


    @staticmethod
    def getAdminAddr(capabilities):
        return ActorAddress(UDPv4ActorAddress(None, capabilities.get('Admin Port', 1029),
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
        convAddr = capabilities.get('Convention Address.IPv4', None)
        if not convAddr:
            return None
        try:
            return UDPTransport.getAddressFromString(convAddr)
        except Exception as ex:
            thesplog('Invalid UCP convention address "%s": %s', convAddr, ex,
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
            ss.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
        #KWQ: need to verify child is started here (message to host) so that the next step (ThespianStatusReq)?  But this would block the parent here waiting for the child when there's other work to be done.  Really want to handle this via run waiting for a natural response from the child.  Similar to MultiProcAdmin handling of h_EndpointConnected?  NEed to do that similar thing in systemBase?

            # sresp, _ign1, _ign2 = select.select([self.socket.fileno()], [], [],
            #                                     None if time_to_quit is None else
            #                                     timePeriodSeconds(time_to_quit -
            #                                                       datetime.now()))
        # KWQ: not actually connected? waiting for child to callback and confirm?  happens automatically?  need this method really?


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
        # Under some python implementations, signal handling (which
        # could generate an ActorShutdownRequest) can be performed
        # without interrupting the underlying syscall, so this message
        # is otherwise ignored but causes the select.select below to
        # return.
        self.socket.sendto(b'BuMP', self.myAddress.addressDetails.sockname)


    def _scheduleTransmitActual(self, transmitIntent):
        if transmitIntent.targetAddr == self.myAddress:
            self._rcvd.append(ReceiveEnvelope(transmitIntent.targetAddr,
                                              transmitIntent.message))
            self.interrupt_wait()
            r = True
        else:
            # UDPTransport transmit is serially blocking, but both sender
            # and receiver provide lots of buffering.  At present, there
            # is no receipt confirmation (KWQ: but there should be)
            r = self.socket.sendto(transmitIntent.serMsg, transmitIntent.targetAddr.addressDetails.sockname)
        transmitIntent.tx_done(SendStatus.Sent
                               if r else
                               SendStatus.BadPacket)


    def _runWithExpiry(self, incomingHandler):
        if incomingHandler == TransmitOnly or \
           isinstance(incomingHandler, TransmitOnly):
            # transmits are not queued/multistage in this transport, no waiting
            return 0

        self._aborting_run = False

        while not self.run_time.expired() and not self._aborting_run:
            if self._rcvd:
                rcvdEnv = self._rcvd.pop()
            else:
                next_action_timeout = self.check_pending_actions()
                try:
                    sresp, _ign1, _ign2 = select.select([self.socket.fileno()], [], [],
                                                        min(self.run_time, next_action_timeout)
                                                        .remainingSeconds())
                except select.error as se:
                    import errno
                    if se.args[0] != errno.EINTR:
                        thesplog('Error during select: %s', se)
                        return None
                    continue
                except ValueError:
                    # self.run_time can expire between the while test
                    # and the use in the select statement.
                    continue

                if [] == sresp:
                    if [] == _ign1 and [] == _ign2:
                        # Timeout, give up
                        return None
                    thesplog('Waiting for read event, but got %s %s', _ign1, _ign2, level=logging.WARNING)
                    continue
                rawmsg, sender = self.socket.recvfrom(65535)
                if rawmsg == b'BuMP':
                    sendAddr = self.myAddress
                    if self._checkChildren:
                        self._checkChildren = False
                        msg = ChildMayHaveDied()
                    elif self._shutdownSignalled:
                        self._shutdownSignalled = False
                        msg = ActorExitRequest()
                    else:
                        return Thespian__UpdateWork()
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
            if not incomingHandler(rcvdEnv):
                return  # handler returned False, indicating run() should exit

        return None

    def check_pending_actions(self):
        expired, remaining = partition(lambda E: E[0].expired(),
                                       self._pending_actions)
        for each in expired:
            each[1]()
        self._pending_actions = remaining
        return min([E[0] for E in self._pending_actions] + [ExpirationTimer(None)])

    def abort_run(self, drain=False):
        """Indicates that run should exit; similar to a handler returning
           False except this can be called from anywhere.  If
           drain=True, then the run() will wait (a reasonable amount
           of time) for system-related messages to be transmitted
           before returning, otherwise run() will terminate as soon as
           control returns to it from this call.
        """
        # UDPTransport does not queue transmits but handles them inline, so no draining required.
        self._aborting_run = True

