"This module provides various low-level inter-Actor transport implementations."

from datetime import timedelta
from thespian.system.timing import ExpirationTimer, currentTime
from thespian.system.utilis import thesplog
import logging


DEFAULT_MAX_TRANSMIT_PERIOD = timedelta(minutes=5)
TRANSMIT_RETRY_PERIOD       = timedelta(seconds=35)
MAX_TRANSMIT_RETRIES        = 20
MAX_SHOWLEN                 = 150
MAX_BACKOFF_DELAY = timedelta(seconds=7, milliseconds=329)
MIN_BACKOFF_DELAY = timedelta(milliseconds=20)
BACKOFF_FACTOR = 1.7


class Thespian__UpdateWork(object):
    """Returned from the transmit run() method to cause the transmit send
       to be called with this same object.  This object is not
       actually transmitted, but this send causes the transmit queues
       to be checked in the context of the main thread) that has a
       chance of seeing alternative work (like a signal-driven exit
       request).
    """
    pass


class Thespian__Run__Result(object):
    """Base class for values returned from the transport run() method.  In
       general, a truthy value means continue and a false-ish value
       (the default) means halt.
    """
    def __nonzero__(self): return False
    def __bool__(self): return False


class Thespian__Run_Expired(Thespian__Run__Result):
    """Returned from the transport run() method if the run time has expired."""
    pass


class Thespian__Run_Terminated(Thespian__Run__Result):
    """Returned from the transport run() method if the transport has been
       shutdown and terminated and is no longer functional."""
    pass


class Thespian__Run_Errored(Thespian__Run_Terminated):
    """Returned from the transport run() method if an internal error has
       occurred.  Usually terminal"""
    def __init__(self, err):
        self.error = err


class Thespian__Run_HandlerResult(Thespian__Run__Result):
    """Returned handler result (false-ish).  Individual handlers should
       return a simple value that the transport's run method wraps in
       this object.
    """
    def __init__(self, val):
        self.return_value = val
    def __nonzero__(self): return self.return_value != 0
    def __bool__(self): return bool(self.return_value)


# ----------------------------------------------------------------------

class TransportInit__Base(object): pass
class ExternalInterfaceTransportInit(TransportInit__Base):
    """Used as first argument to Transport __init__ to indicate that this
       is an external process interfacing to the ActorSystem via the
       Transport.
    """
    pass


class TransmitOnly(object):
    """Passed *as a class* to transport.run as the "handler" to indicate
       that no incoming message processing should occur and as soon as
       a transmit completes, returning the number of remaining
       transmits queued in the transport layer.

       If there are no queued transmits in the transport layer, the run()
       call returns immediately with a value of 0.

       Note that the transport layer may handle multiple transmits in
       parallel; calling run() with this argument may allow several
       transmits to progress through transmit stages---possibly even
       to completion.  The run() return indicates only that a single
       transmit has completed and should be called soon if there are
       still transmits pending to complete their transmit progress.

       Also note that the timeout argument to the run() method can
       cause it to return without actually completing any transmits.
    """
    pass


# ----------------------------------------------------------------------

class ReceiveEnvelope(Thespian__Run__Result):
    "Represents the message received along with the sender's address"
    def __init__(self, sender, msg):
        self._sender  = sender
        self._message = msg
    @property
    def sender(self): return self._sender
    @property
    def message(self): return self._message
    def identify(self):
        try:
            smsg = str(self.message)
        except Exception:
            smsg = "<message>"
        if len(smsg) > MAX_SHOWLEN:
            smsg = smsg[:MAX_SHOWLEN] + '...'
        msgt = str(type(self.message))
        if smsg == msgt:
            return 'ReceiveEnvelope(from: %s, msg: %s)'%(self.sender, smsg)
        return 'ReceiveEnvelope(from: %s, %s msg: %s)'%(self.sender, msgt, smsg)
    def __str__(self): return self.identify()

    # As a Thespian__Run__Result, this is false-ish because the caller
    # supplied no receive handler, so the run should stop looping and
    # return this value to the caller.
    def __nonzero__(self): return False
    def __bool__(self): return False


# ----------------------------------------------------------------------

class ResultCallback(object):
    def __init__(self, onSuccess=None, onFailure=None, nextCallback=None):
        self._successTo = onSuccess
        self._failureTo = onFailure
        self._thenTo    = nextCallback
        self._called    = False

    def resultCallback(self, withResult, withValue):
        """This is called by the transport to perform the success or failure
           callback operation.  Exceptions are swallowed and do not
           escape.  All callbacks in the chain are called in sequence.
        """
        if not self._called:
            self._called = True
            try:
                ((self._successTo
                  if withResult else
                  self._failureTo) or (lambda r, m: None))(withResult, withValue)
            except Exception as ex:
                thesplog('Exception in callback: %s', ex, exc_info=True, level=logging.ERROR)
                # Ensure additional callbacks are still called even if a callback gets an exceptions.
            if self._thenTo:
                self._thenTo.resultCallback(withResult, withValue)


# ----------------------------------------------------------------------


def backoffDelay(curDelay=0):
    adjtime = curDelay or MIN_BACKOFF_DELAY
    if not isinstance(adjtime, timedelta): adjtime = timedelta(seconds=adjtime)
    return min(MAX_BACKOFF_DELAY,
               timedelta(days = adjtime.days * BACKOFF_FACTOR,
                         seconds = (adjtime.seconds * BACKOFF_FACTOR),
                         microseconds = (adjtime.microseconds * BACKOFF_FACTOR)))


class PauseWithBackoff(object):
    def backoffPause(self, startPausing=False):
        if startPausing:
            self._lastPauseLength = backoffDelay(getattr(self, '_lastPauseLength', 0))
            self._pauseUntil = ExpirationTimer(self._lastPauseLength)
            return self._lastPauseLength
        elif hasattr(self, '_pauseUntil'):
            with self._pauseUntil as pausing:
                if not pausing.expired():
                    return pausing.remaining()
            delattr(self, '_pauseUntil')
        return timedelta(0)


# ----------------------------------------------------------------------

class TransmitIntent(PauseWithBackoff):
    """An individual transmission of data can be encapsulated by a
       "transmit intent", which identifies the message and the target
       address, and which has a callback for eventual success or
       failure indication.  Transmit intents may be chained together
       to represent a series of outbound transmits.  Adding a transmit
       intent to the chain may block when the chain reaches an upper
       threshold, and remain blocked until enough transmits have
       occured (successful or failed) to reduce the size of the chain
       below a minimum threshold.  This acts to implement server-side
       flow control in the system as a whole (although it can
       introduce a deadlock scenario if multiple actors form a
       transmit loop that is blocked at any point in the loop, so a
       transmit intent will fail if it reaches a maximum number of
       retries without success).

       The TransmitIntent is constructed with a target address, the
       message to send, and optional onSuccess and onError callbacks
       (both defaulting to None).  The callbacks are passed the
       TransmitIntent when the transport is finished with it,
       selecting the appropriate callback based on the completion
       status (the `result' property will reveal the SendStatus actual
       result of the attempt).  A callback of None will simply discard
       the TransmitIntent without passing it to a callback.

       The TransmitIntent is passed to the transport that should
       perform the intent; the transport may attach its own additional
       data to the intent during that processing.

    """

    def __init__(self, targetAddr, msg, onSuccess=None, onError=None, maxPeriod=None,
                 retryPeriod=TRANSMIT_RETRY_PERIOD):
        super(TransmitIntent, self).__init__()
        self._targetAddr = targetAddr
        self._message    = msg
        self._callbackTo = ResultCallback(onSuccess, onError)
        self._resultsts  = None
        self._quitTime   = ExpirationTimer(maxPeriod or DEFAULT_MAX_TRANSMIT_PERIOD)
        self._attempts    = 0
        self.transmit_retry_period = retryPeriod

    @property
    def targetAddr(self): return self._targetAddr
    @property
    def message(self): return self._message

    def changeTargetAddr(self, newAddr): self._targetAddr = newAddr
    def changeMessage(self, newMessage): self._message = newMessage

    @property
    def result(self): return self._resultsts
    @result.setter
    def result(self, setResult):
        if not isinstance(setResult, SendStatus.BASE):
            raise TypeError('TransmitIntent result must be a SendStatus (got %s)'%type(setResult))
        self._resultsts = setResult

    def completionCallback(self):
        "This is called by the transport to perform the success or failure callback operation."
        if not self.result:
            if self.result == SendStatus.DeadTarget:
                # Do not perform logging in case admin or logdirector
                # is dead (this will recurse infinitely).
                # logging.getLogger('Thespian').warning('Dead target: %s', self.targetAddr)
                pass
            else:
                thesplog('completion error: %s', str(self), level=logging.INFO)
        self._callbackTo.resultCallback(self.result, self)

    def addCallback(self, onSuccess=None, onFailure=None):
        self._callbackTo = ResultCallback(onSuccess, onFailure, self._callbackTo)


    def tx_done(self, status):
        self.result = status
        self.completionCallback()


    def awaitingTXSlot(self):
        self._awaitingTXSlot = True


    def retry(self, immediately=False):
        if self._attempts > MAX_TRANSMIT_RETRIES:
            return False
        if self._quitTime.view().expired():
            return False
        self._attempts += 1
        if immediately:
            self._retryTime = ExpirationTimer(0)
        else:
            self._retryTime = ExpirationTimer(self._attempts * self.transmit_retry_period)
        return True

    def timeToRetry(self, socketAvail=False):
        if socketAvail and hasattr(self, '_awaitingTXSlot'):
            delattr(self, '_awaitingTXSlot')
            if hasattr(self, '_retryTime'):
                delattr(self, '_retryTime')
            return True
        if hasattr(self, '_retryTime'):
            retryNow = self._retryTime.view().expired()
            if retryNow:
                delattr(self, '_retryTime')
            return retryNow
        return socketAvail

    def delay(self, current_time = None):
        ct = current_time or currentTime()
        qt = self._quitTime.view(ct)
        if getattr(self, '_awaitingTXSlot', False):
            if qt.expired():
                return timedelta(seconds=0)
            return max(timedelta(milliseconds=10), (qt.remaining()) / 2)
        return max(timedelta(seconds=0),
                   min(qt.remaining(),
                       getattr(self, '_retryTime', self._quitTime).view(ct).remaining(),
                       getattr(self, '_pauseUntil', self._quitTime).view(ct).remaining()))

    def expired(self):
        return self._quitTime.view().expired()

    def expiration(self):
        return self._quitTime

    def __str__(self):
        return '************* %s' % self.identify()

    def identify(self):
        try:
            smsg = str(self.message)
        except Exception:
            smsg = '<msg-cannot-convert-to-ascii>'
        if len(smsg) > MAX_SHOWLEN:
            smsg = smsg[:MAX_SHOWLEN] + '...'
        return 'TransportIntent(' + '-'.join(filter(None, [
            str(self.targetAddr),
            'pending' if self.result is None else '='+str(self.result),
            '' if self.result is not None else 'ExpiresIn_' + str(self.delay()),
            'WAITSLOT' if getattr(self, '_awaitingTXSlot', False) else None,
            'retry#%d'%self._attempts if self._attempts else '',
            str(type(self.message)), smsg,
            'quit_%s'%str(self._quitTime.view().remaining()),
            'retry_%s'%str(self._retryTime.view().remaining()) if getattr(self, '_retryTime', None) else None,
            'pause_%s'%str(self._pauseUntil.view().remaining()) if getattr(self, '_pauseUntil', None) else None,
            ])) + ')'


class SendStatus(object):
    class BASE(object):
        _isGood = True
        def __bool__(self):    return self._isGood  # Python3
        def __nonzero__(self): return self._isGood  # Python2
        def __str__(self): return '-+'[bool(self)]+self.__class__.__name__
    class SENDSTS_SENT(BASE): pass
    class SENDSTS_NOTSENT(BASE):
        "Has not been sent, has not been actively rejected; still pending usually"
        _isGood = False
    class BadPacketError(BASE, Exception):
        "Remote rejected transmit, (a return value or an exception)"
        _isGood = False
    class SENDSTS_EXPIRED(BASE):
        "Transmit intent expired before send completed."
        _isGood = False
    class SENDSTS_FAILED(BASE): _isGood = False
    class SENDSTS_DEADTARGET(BASE): _isGood = False
    Sent = SENDSTS_SENT()
    NotSent = SENDSTS_NOTSENT()
    BadPacket = BadPacketError('BadPacket SendStatus')
    Failed = SENDSTS_FAILED()
    Expired = SENDSTS_EXPIRED()
    DeadTarget = SENDSTS_DEADTARGET()


class ForwardMessage(object):
    "Used as a wrapper when forwarding messages via intermediaries"
    # n.b. ForwardMessage is not based the ActorSystemMessage base class
    # because it only exists at the transport layer.

    def __init__(self, fwdMessage, fwdTo, fwdFrom, fwdChain=None):
        self.fwdMessage = fwdMessage
        self.fwdTo      = fwdTo  # final destination
        self.fwdFrom    = fwdFrom  # original sender
        self.fwdTargets = (fwdChain or []) + [fwdTo]  # list of targets; last is fwdTo
    def __str__(self):
        return 'FWD(%s)%s->%s->%s'%(str(self.fwdMessage),
                                    str(self.fwdFrom),
                                    '->'.join(list(map(str, self.fwdTargets))),
                                    str(self.fwdTo))
