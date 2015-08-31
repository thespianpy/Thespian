"""Simple TCP sockets.

Each Actor has a TCP IPv4 port/socket that will accept incoming
connections for messages.  Each connection from a remote Actor will
accept a single message per connection.  The connection is dropped and
re-established for multiple messages; this is less efficient but has
more fairness.

This transport can be used within a process, between processes, and
even between processes on separate systems.

"""

# n.b. The core of this is very similar to asyncore/asynchat.
# Unfortunately, those modules are deprecated in Python 3.4 in favor
# of asyncio, which is powerful... and complex.  Thespian aims to
# support Python 2.6 through 3.4 and beyond, and has more specific
# needs (undoubtably a subset of asyncio capabilities) that can be
# implemented more simply and directly here.  In addition, this module
# should be extensible to support SSL, which asynchat is not (maybe).

# For Thespian, there are two classes of sockets:
#   * the Actor's primary receive socket, and
#   * transient outgoing send sockets.
# All steps of both types of sockets are handled asynchronously, very
# similarly to the asyncore channel.
#
# For the receive socket, it will listen for and accept incoming
# connections, and then accept a single message on that connection,
# closing the connection on completion (or error).
#
# For the transmit socket, it will connect, send, and close with a
# TransmitIntent.

# ----------------------------------------------------------------------

# TCP Buffering issues
#
# TCP is unique in that there are unusual buffering considerations to
# account for.  Specifically, a sender can connect to a listener, send
# a message, and close the socket --- *WITHOUT the receiver even
# processing the accept!  As a result, the transmitter must take
# additional steps to ensure that the message that was sent has been
# delivered.
#
# There are two ways that this confirmation can be handled:
#
#  1) confirmation sent back in the original connection
#
#  2) messages confirmed by a separate confirmation message with a
#     unique message identifier for idempotency.
#
# Disadvantages of #1:
#   * More complicated exchange between sender and receiver
#   * There must be a header synchronization with a size indicator so
#     that the receiver knows when the full message has been received
#     and should be acknowledged.
#   * The socket must exist for a potentially much longer period and
#     retransmits must still be attempted on failure.
#
# Disadvantages of #2:
#   * Doubles connection establishment requirements.
#   * More complicated send queuing to ensure ordering of messages between
#     sender and recipient.  However, this really must exist for condition
#     #1 as well.
#
# Could also do a hybrid of both.  On send, start with header
# containing message ID (and size?) then wait a brief time after send
# for the ACK, then disconnect and wait for the separate ACK later.
#
# At this point, the connection establishment seems to be the
# overriding performance dominator, and the message header
# synchronization and size indication seem like good ideas anyhow to
# confirm that the entire message has been received by the recipient.
# This method is feasible because of the asynchronous handling of the
# transmit sequence (as opposed to a blocking transmit, which would
# consume the processing budget for highly active scenarios).


import logging
from thespian.system.utilis import (timePeriodSeconds, ExpiryTime, thesplog,
                                    fmap, partition)
from thespian.actors import *
from thespian.system.transport import *
from thespian.system.transport.IPBase import TCPv4ActorAddress
from thespian.system.transport.streamBuffer import (toSendBuffer, ReceiveBuffer,
                                                    ackMsg, ackPacket,
                                                    ackDataErrMsg, ackDataErrPacket)
from thespian.system.transport.asyncTransportBase import asyncTransportBase
from thespian.system.transport.wakeupTransportBase import wakeupTransportBase
from thespian.system.addressManager import ActorLocalAddress
import socket
import select
from datetime import datetime, timedelta
#import json
import pickle
import errno


serializer = pickle
# json cannot be used because Messages are often structures, which cannot be converted to JSON.

LISTEN_DEPTH=100  # max # of listens to sign up for at a time
MAX_INCOMING_SOCKET_PERIOD=timedelta(minutes=7)  # max time to hold open an incoming socket
MAX_CONSECUTIVE_READ_FAILURES = 20


class TCPEndpoint(TransportInit__Base):
    def __init__(self, *args): self.args = args
    @property
    def addrInst(self): return self.args[0]


def _safeSocketShutdown(sock):
    try:
        sock.shutdown(socket.SHUT_RDWR) # KWQ: all these should be protected!
    except socket.error as ex:
        if ex.errno != errno.ENOTCONN:
            thesplog('Error during shutdown of socket %s: %s', sock, ex)
    sock.close()


class TCPIncoming(PauseWithBackoff):
    def __init__(self, acceptResponse):
        super(TCPIncoming, self).__init__()
        self._aResp = acceptResponse
        self._rData = ReceiveBuffer(serializer.loads)
        self._expires = datetime.now() + MAX_INCOMING_SOCKET_PERIOD
        self._aResp[0].setblocking(0)
        # Disable Nagle to transmit headers and acks asap
        self._aResp[0].setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._openSock = True
        self.failCount = 0
    @property
    def socket(self): return self._aResp[0] if self._openSock else None
    @property
    def fromAddress(self): return ActorAddress(TCPv4ActorAddress(*(self._aResp[1])))
    def delay(self):
        now = datetime.now()
        # n.b. include _pauseUntil from PauseWithBackoff
        return max(timedelta(seconds=0),
                   min(self._expires - now,
                       getattr(self, '_pauseUntil', self._expires) - now))
    def addData(self, newData): self._rData.addMore(newData)
    def remainingSize(self): return self._rData.remainingAmount()
    def receivedAllData(self): return self._rData.isDone()
    @property
    def data(self): return self._rData.completed()[0]
    def close(self):
        s = self.socket
        if s:
            self._openSock = False
            _safeSocketShutdown(s)
    def __del__(self):
        s = self.socket
        if s:
            self._openSock = False
            _safeSocketShutdown(s)
    def __str__(self): return 'TCPInc(%s)<%s>'%(str(self._aResp), str(self._rData))


class TCPTransport(asyncTransportBase, wakeupTransportBase):
    "A transport using TCP IPv4 sockets for communications."

    def __init__(self, initType, *args):
        super(TCPTransport, self).__init__()

        if isinstance(initType, ExternalInterfaceTransportInit):
            # External process that is going to talk "in".  There is
            # no parent, and the child is the systemAdmin.
            capabilities, logDefs = args
            convAddr = capabilities.get('Convention Address.IPv4', '')
            if convAddr and type(convAddr) == type( (1,2) ):
                externalAddr = convAddr
            elif type(convAddr) == type("") and ':' in convAddr:
                externalAddr = convAddr.split(':')
                externalAddr = externalAddr[0], int(externalAddr[1])
            else:
                externalAddr          = (convAddr, capabilities.get('Admin Port', 1900))
            templateAddr          = ActorAddress(TCPv4ActorAddress(None, 0, external = externalAddr))
            self._adminAddr       = self.getAdminAddr(capabilities)
            self._parentAddr      = None
        elif isinstance(initType, TCPEndpoint):
            instanceNum, assignedAddr, self._parentAddr, self._adminAddr = initType.args
            templateAddr = assignedAddr or ActorAddress(TCPv4ActorAddress(None, 0,
                                                                          external = (self._parentAddr or
                                                                                      self._adminAddr or
                                                                                      True)))

        else:
            thesplog('TCPTransport init of type %s unsupported', type(initType), level=logging.ERROR)
            raise ActorSystemStartupFailure('Invalid TCPTransport init type (%s)'%type(initType))

        self.socket = socket.socket(*templateAddr.addressDetails.socketArgs)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(*templateAddr.addressDetails.bindArgs)
        self.socket.listen(LISTEN_DEPTH)
        # N.B.  myAddress is actually the address we will export for
        # others to talk to us, not the bind address.  The difference
        # is that we bind to '0.0.0.0' (inaddr_any), but that's not a
        # valid address for people to send stuff to us.  The
        # self.socket socket name is likely inaddr_any but has the
        # valid port, whereas the templateAddr has our actual public
        # address.
        self.myAddress = ActorAddress(TCPv4ActorAddress(
                templateAddr.addressDetails.connectArgs[0][0],
                self.socket.getsockname()[1],
                 external = True))
        self._transmitIntents = []
        self._incomingSockets = []
        self._incomingEnvelopes = []

    def __del__(self):
        if hasattr(self, 'socket') and self.socket:
            _safeSocketShutdown(self.socket)


    def protectedFileNumList(self):
        return fmap(lambda s: s.fileno(),
                     filter(None, [self.socket] +
                            [I.socket for I in self._incomingSockets] +
                            [I.socket for I in self._transmitIntents
                             if hasattr(I, 'socket')]))

    def childResetFileNumList(self):
        return self.protectedFileNumList()


    @staticmethod
    def getAdminAddr(capabilities):
        return ActorAddress(TCPv4ActorAddress(None, capabilities.get('Admin Port', 1900),
                                              external = (TCPTransport.getConventionAddress(capabilities) or
                                                          ('', capabilities.get('Admin Port', 1900)) or
                                                          True)))

    @staticmethod
    def getAddressFromString(addrspec):
        if isinstance(addrspec, tuple):
            addrparts = addrspec
        else:
            addrparts = addrspec.split(':')
        if 1 == len(addrparts):
            return ActorAddress(TCPv4ActorAddress(addrparts[0], 1900, external=True))
        return ActorAddress(TCPv4ActorAddress(addrparts[0], addrparts[1], external=True))

    @staticmethod
    def getConventionAddress(capabilities):
        convAddr = capabilities.get('Convention Address.IPv4', None)
        if not convAddr:
            return None
        try:
            return TCPTransport.getAddressFromString(convAddr)
        except Exception as ex:
            thesplog('Invalid TCP convention address "%s": %s', convAddr, ex,
                     level=logging.ERROR)
            raise InvalidActorAddress(convAddr, str(ex))


    def _updateStatusResponse(self, resp):
        "Called to update a Thespian_SystemStatus or Thespian_ActorStatus with common information"
        for each in self._transmitIntents:
            resp.addPendingMessage(self.myAddress, each.targetAddr, str(each.message))
        for each in self._incomingEnvelopes:
            resp.addReceivedMessage(each.sender, self.myAddress, str(each.message))
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
                if ex.errno == errno.EADDRINUSE:
                    return True
                # Some other error... not sure if that means an admin is running or not.
                return False  # assume not
        finally:
            ss.close()


    def prepEndpoint(self, assignedLocalAddr):
        """In the parent, prepare to establish a new communications endpoint
           with a new Child Actor.  The result of this call will be
           passed to a created child process to use when initializing
           the Transport object for that class; the result of this
           call will also be kept by the parent to finalize the
           communications after creation of the Child by calling
           connectEndpoint() with this returned object.
        """
        if isinstance(assignedLocalAddr.addressDetails, ActorLocalAddress):
            return TCPEndpoint(assignedLocalAddr.addressDetails.addressInstanceNum,
                               None,
                               self.myAddress,
                               self._adminAddr)
        return TCPEndpoint(None,
                           assignedLocalAddr,  # assumed to be an actual TCPActorAddress-based address (e.g. admin)
                           self.myAddress,
                           self._adminAddr)

    def connectEndpoint(self, endPoint):
        pass


    def deadAddress(self, addressManager, childAddr):
        canceli, continuei = partition(lambda i: i.targetAddr == childAddr,
                                       self._transmitIntents)
        self._transmitIntents = continuei
        for each in canceli:
            if hasattr(each, 'socket'):
                each.socket.close()
                delattr(each, 'socket')
            each.result = SendStatus.DeadTarget
            each.completionCallback()

        # No need to clean up self._incomingSockets entries: they will timeout naturally
        super(TCPTransport, self).deadAddress(addressManager, childAddr)


    _XMITStepSendConnect    = 1
    _XMITStepSendData       = 2
    _XMITStepShutdownWrite  = 3
    _XMITStepWaitForAck     = 4
    _XMITStepCloseAndFinish = 5
    _XMITStepRetry          = 6

    def serializer(self, intent):
        return toSendBuffer((self.myAddress, intent.message), serializer.dumps)

    def _scheduleTransmitActual(self, intent):
        if intent.targetAddr == self.myAddress:
            self._incomingEnvelopes.append(ReceiveEnvelope(intent.targetAddr, intent.message))
            return self._finishIntent(intent)
        intent.stage = self._XMITStepSendConnect
        if self._nextTransmitStep(intent):
            self._transmitIntents.append(intent)

    def _finishIntent(self, intent, status=SendStatus.Sent):
        if hasattr(intent, 'socket'):
            _safeSocketShutdown(intent.socket)
            delattr(intent, 'socket')
        intent.result = status
        intent.completionCallback()
        return False  # intent no longer needs to be attempted

    def _nextTransmitStepCheck(self, intent, fileno):
        if (hasattr(intent, 'socket') and intent.socket.fileno() == fileno) or \
           (fileno == -1 and intent.timeToRetry()):
            return self._nextTransmitStep(intent)
        if not intent.delay():
            # Transmit timed out (consider this permanent)
            thesplog('Transmit attempt from %s to %s timed out, returning PoisonPacket',
                     self.myAddress, intent.targetAddr, level=logging.WARNING)
            #self._incomingEnvelopes.append(ReceiveEnvelope(intent.targetAddr,
            #                                               PoisonPacket(intent.message)))
            # Stop attempting this transmit
            return self._finishIntent(intent, SendStatus.Failed)
        # Continue to attempt this transmit
        return True

    def _nextTransmitStep(self, intent):
        # Return of True means keep waiting on this Transmit Intent; False means it is done
        try:
            return getattr(self, '_next_XMIT_%s'%intent.stage, '_unknown_XMIT_step')(intent)
        except Exception as ex:
            import traceback
            thesplog('xmit UNcaught exception %s; aborting intent.\n%s',
                     ex, traceback.format_exc(), level=logging.ERROR)
            return False

    def _next_XMIT_1(self, intent):
        intent.socket = socket.socket(*intent.targetAddr.addressDetails.socketArgs)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        intent.socket.setblocking(0)
        # Disable Nagle to transmit headers and acks asap; our sends are usually small
        intent.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        intent.socket.settimeout(timePeriodSeconds(intent.delay()))
        try:
            intent.socket.connect(*intent.targetAddr.addressDetails.connectArgs)
        except socket.error as err:
            # EINPROGRESS means non-blocking socket connect is in progress...
            if err.errno != errno.EINPROGRESS:
                thesplog('Socket connect failure %s to %s (returning %s)',
                         err, intent.targetAddr, intent.completionCallback,
                         level=logging.WARNING)
                return self._finishIntent(intent,
                                          SendStatus.DeadTarget \
                                          if err.errno == errno.ECONNREFUSED \
                                          else SendStatus.Failed)
            intent.backoffPause(True)
        except Exception as ex:
            thesplog('Unexpected TCP socket connect exception: %s', ex,
                     level=logging.ERROR)
            return self._finishIntent(intent, SendStatus.BadPacket)
        intent.stage = self._XMITStepSendData # When connect completes
        intent.amtSent = 0
        return True

    def _next_XMIT_2(self, intent):
        # Got connected, ready to send
        try:
            #intent.socket.sendall(intent.serMsg)
            intent.amtSent += intent.socket.send(intent.serMsg[intent.amtSent:])
        except socket.error as err:
            if err.errno in [errno.EINPROGRESS, errno.EAGAIN]:
                intent.backoffPause(True)
                return True
            if err.errno == errno.ECONNREFUSED:
                # in non-blocking, sometimes connection attempts are
                # discovered here rather than for the actual connect
                # request.
                thesplog('ConnRefused to %s; declaring as DeadTarget.',
                         intent.targetAddr, level=logging.ERROR)
                return self._finishIntent(intent, SendStatus.DeadTarget)
            thesplog('Socket error sending: %s / %s', str(err), err.errno, level=logging.ERROR)
            intent.stage = self._XMITStepRetry
            return self._nextTransmitStep(intent)
        except Exception:
            import traceback
            thesplog('Error sending: %s', traceback.format_exc(), level=logging.ERROR)
            intent.stage = self._XMITStepRetry
            return self._nextTransmitStep(intent)
        if intent.amtSent >= len(intent.serMsg):
            intent.stage = self._XMITStepShutdownWrite  # After data is sent, stop transmit
        return True

    def _next_XMIT_3(self, intent):
        try:
            pass
            # Original did a socket shutdown for writing, but actual
            # socket implementations aren't so sophisticated and this
            # tended to stop all socket communications in both
            # directions.
            # intent.socket.shutdown(socket.SHUT_WR)
        except socket.error:
            # No shutdown handling, just close
            intent.stage = self._XMITStepCloseAndFinish
            return self._nextTransmitStep(intent)
        intent.ackbuf = ReceiveBuffer(serializer.loads)
        intent.stage = self._XMITStepWaitForAck
        return True

    def _next_XMIT_4(self, intent):
        # Actually, select below waited on readable, not writeable
        try:
            rcv = intent.socket.recv(intent.ackbuf.remainingAmount())
        except socket.error as err:
            if errno.ECONNRESET == err.errno:
                thesplog('Remote %s closed connection before ack received at %s',
                         str(intent.targetAddr), str(self.myAddress),
                         level=logging.WARNING)
            else:
                thesplog('Socket Error waiting for transmit ack from %s to %s: %s',
                         str(intent.targetAddr), str(self.myAddress), err,
                         level=logging.ERROR, exc_info=True)
            rcv = ''  # Remote closed connection
        except Exception as err:
            thesplog('General error waiting for transmit ack from %s to %s: %s',
                     str(intent.targetAddr), str(self.myAddress), err,
                     level=logging.ERROR, exc_info=True)
            rcv = ''  # Remote closed connection
        if rcv:
            intent.ackbuf.addMore(rcv)
            if not intent.ackbuf.isDone():
                # Continue waiting for ACK
                return True
            ackmsg = intent.ackbuf.completed()[0]
            if ackmsg in [ackPacket, ackDataErrPacket]:
                intent.result = SendStatus.Sent if ackmsg == ackPacket \
                                else SendStatus.BadPacket
                intent.stage = self._XMITStepCloseAndFinish
                return self._nextTransmitStep(intent)
            thesplog('Unrecognized ACK packet')
        # Invalid ack, or no receive but socket closed.  Reschedule transmit.
        thesplog('Rescheduling transmit')
        intent.backoffPause(True)
        intent.stage = self._XMITStepRetry
        return self._nextTransmitStep(intent)

    def _next_XMIT_5(self, intent):
        return self._finishIntent(intent, intent.result)

    def _next_XMIT_6(self, intent):
        if hasattr(intent, 'socket'):
            _safeSocketShutdown(intent.socket)
            delattr(intent, 'socket')
        if hasattr(intent, 'ackbuf'): delattr(intent, 'ackbuf')
        if intent.retry():
            intent.stage = self._XMITStepSendConnect
            # stage just set won't be executed until retry delay times out
            return True
        return self._finishIntent(intent, SendStatus.Failed)


    @staticmethod
    def _waitForSendable(sendIntent):
        return sendIntent.stage != TCPTransport._XMITStepWaitForAck


    @staticmethod
    def _socketFile(sendOrRecv):
        return sendOrRecv.socket.fileno() if getattr(sendOrRecv, 'socket', None) else None


    def _runWithExpiry(self, incomingHandler):
        xmitOnly = incomingHandler == TransmitOnly or \
                   isinstance(incomingHandler, TransmitOnly)

        if hasattr(self, '_aborting_run'): delattr(self, '_aborting_run')

        while not self.run_time.expired() and \
              (not hasattr(self, '_aborting_run') or
               (self._aborting_run and self._transmitIntents)):

            if xmitOnly:
                if not self._transmitIntents:
                    return 0
            else:
                while self._incomingEnvelopes:
                    rEnv = self._incomingEnvelopes.pop(0)
                    if incomingHandler is None:
                        return rEnv
                    if not incomingHandler(rEnv):
                        return None

            wsend, wrecv = fmap(TCPTransport._socketFile,
                                partition(TCPTransport._waitForSendable,
                                          filter(lambda T: not T.backoffPause(),
                                                 self._transmitIntents)))

            wrecv = list(filter(None, wrecv))
            wsend = list(filter(None, wsend))
            wrecv.extend(list(filter(None,
                                     [TCPTransport._socketFile(I)
                                      for I in self._incomingSockets
                                      if not I.backoffPause()])))

            delays = list([R for R in [self.run_time.remaining()] +
                           [T.delay() for T in self._transmitIntents] +
                           [T.delay() for T in self._incomingSockets]
                           if R is not None])
            delay = timePeriodSeconds(min(delays)) if delays else None

            if not hasattr(self, '_aborting_run') and not xmitOnly:
                wrecv.extend([self.socket.fileno()])
            # rrecv, rsend, _ign2 = select.select(wrecv, wsend, [], delay)
            try:
                rrecv, rsend, _ign2 = select.select(wrecv, wsend, set(wsend+wrecv),
                                                    delay)
            except ValueError as ex:
                thesplog('ValueError on select(#%d: %s, #%d: %s, #%d: %s, %s)',
                         len(wrecv), wrecv, len(wsend), wsend,
                         len(set(wsend + wrecv)), set(wsend + wrecv),
                         delay, level=logging.ERROR)
                raise
            except select.error as ex:
                if ex.args[0] in (errno.EINVAL,  # probably a change in descriptors
                                  errno.EINTR,
                              ):
                    thesplog('select retry on %s', ex, level=logging.ERROR)
                    continue
                raise


            if _ign2:
                thesplog('WHOA... something else to do for sockets: %s',
                         _ign2, level=logging.WARNING)

            origPendingSends = len(self._transmitIntents)

            # Handle newly sendable data
            for eachs in rsend:
                self._transmitIntents = [I for I in self._transmitIntents
                                         if self._nextTransmitStepCheck(I, eachs)]

            # Handle newly receivable data
            for each in rrecv:
                if each == self.socket.fileno():
                    self._acceptNewIncoming()
                    continue
                self._incomingSockets = [S for S in self._incomingSockets
                                         if self._handlePossibleIncoming(S, each)]
                self._transmitIntents = [I for I in self._transmitIntents
                                         if self._nextTransmitStepCheck(I, each)]

            # Handle timeouts
            self._transmitIntents = [I for I in self._transmitIntents
                                     if self._nextTransmitStepCheck(I, -1)]
            self._incomingSockets = [S for S in self._incomingSockets
                                     if self._handlePossibleIncoming(S, -1)]

            # Check if it's time to quit
            if [] == rrecv and [] == rsend:
                if [] == _ign2 and self.run_time.expired():
                    # Timeout, give up
                    return None
                continue
            if xmitOnly:
                remXmits = len(self._transmitIntents)
                if origPendingSends > remXmits or remXmits == 0:
                    return remXmits

            # Handle queued internal "received" data
            if not xmitOnly:
                while self._incomingEnvelopes:
                    rEnv = self._incomingEnvelopes.pop(0)
                    if incomingHandler is None:
                        return rEnv
                    if not incomingHandler(rEnv):
                        return None

        return None


    def _acceptNewIncoming(self):
        lsock = self.socket.accept()
        self._incomingSockets.append(TCPIncoming(lsock))


    def _handlePossibleIncoming(self, incomingSocket, fileno):
        if incomingSocket.socket and (incomingSocket.socket.fileno() == fileno or \
                                      not incomingSocket.delay()):
            rval = self._handleReadableIncoming(incomingSocket)
        else:
            rval = incomingSocket.delay() # Continue to wait for incoming on this socket if True
        if not rval:
            incomingSocket.close()
        return rval


    def _handleReadableIncoming(self, inc):
        try:
            rdata = inc.socket.recv(inc.remainingSize())
            inc.failCount = 0
        except socket.error as e:
            inc.failCount = getattr(inc, 'failCount', 0) + 1
            if e.errno in [errno.EAGAIN, errno.EWOULDBLOCK] and inc.failCount < MAX_CONSECUTIVE_READ_FAILURES:
                inc.backoffPause(True)
                return True
            thesplog('Error reading from socket (#%s); closing: %s', inc.failCount, e)
            return False
        if not rdata:
            # Since this point is only arrived at when select() says
            # the socket is readable, this is an indicator of a closed
            # socket.  Since previous calls didn't detect
            # receivedAllData(), this is an aborted/incomplete
            # reception.  Discard it.
            return False
        inc.addData(rdata)
        if not inc.receivedAllData():
            # Continue running and monitoring this socket
            return True
        try:
            rEnv = ReceiveEnvelope(*inc.data)
        except Exception:
            import traceback
            thesplog('OUCH!  Error deserializing received data: %s', traceback.format_exc())
            inc.socket.sendall(ackDataErrMsg)
            # Continue running, but release this socket
            return False
        inc.socket.sendall(ackMsg)
        self._incomingEnvelopes.append(rEnv)
        # Continue to run, but this socket is releasable
        return False


    def abort_run(self, drain=False):
        self._aborting_run = drain
