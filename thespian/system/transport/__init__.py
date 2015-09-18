"This module provides various low-level inter-Actor transport implementations."

from datetime import datetime, timedelta
from thespian.system.utilis import thesplog
import logging


DEFAULT_MAX_TRANSMIT_PERIOD = timedelta(minutes=5)
TRANSMIT_RETRY_PERIOD       = timedelta(seconds=35)
MAX_TRANSMIT_RETRIES        = 20
MAX_SHOWLEN                 = 150
MAX_BACKOFF_DELAY = timedelta(seconds=7, milliseconds=329)
MIN_BACKOFF_DELAY = timedelta(milliseconds=20)
BACKOFF_FACTOR = 1.7


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

class ReceiveEnvelope(object):
    "Represents the message received along with the sender's address"
    def __init__(self, sender, msg):
        self._sender  = sender
        self._message = msg
    @property
    def sender(self): return self._sender
    @property
    def message(self): return self._message
    def identify(self):
        smsg = str(self.message)
        if len(smsg) > MAX_SHOWLEN:
            smsg = smsg[:MAX_SHOWLEN] + '...'
        msgt = str(type(self.message))
        if smsg == msgt:
            return 'ReceiveEnvelope(from: %s, msg: %s)'%(self.sender, smsg)
        return 'ReceiveEnvelope(from: %s, %s msg: %s)'%(self.sender, msgt, smsg)



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
            self._pauseUntil = datetime.now() + self._lastPauseLength
            return self._lastPauseLength
        elif hasattr(self, '_pauseUntil'):
            now = datetime.now()
            if now < self._pauseUntil:
                return self._pauseUntil - now
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
        self._quitTime   = datetime.now() + (maxPeriod or DEFAULT_MAX_TRANSMIT_PERIOD)
        self.nextIntent  = None
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
            thesplog('completion error: %s', str(self), level=logging.INFO)
        self._callbackTo.resultCallback(self.result, self)

    def addCallback(self, onSuccess=None, onFailure=None):
        self._callbackTo = ResultCallback(onSuccess, onFailure, self._callbackTo)

    def retry(self):
        if self._attempts > MAX_TRANSMIT_RETRIES:
            return False
        if self._quitTime < datetime.now():
            return False
        self._attempts += 1
        self._retryTime = datetime.now() + (self._attempts * self.transmit_retry_period)
        return True

    def timeToRetry(self):
        return hasattr(self, '_retryTime') and self._retryTime <= datetime.now()

    def delay(self):
        now = datetime.now()
        return max(timedelta(seconds=0),
                   min(self._quitTime - now,
                       getattr(self, '_retryTime', self._quitTime) - now,
                       getattr(self, '_pauseUntil', self._quitTime) - now))

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
            'retry#%d'%self._attempts if self._attempts else '',
            str(type(self.message)), smsg
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
    class SENDSTS_FAILED(BASE): _isGood = False
    class SENDSTS_DEADTARGET(BASE): _isGood = False
    Sent = SENDSTS_SENT()
    NotSent = SENDSTS_NOTSENT()
    BadPacket = BadPacketError('BadPacket SendStatus')
    Failed = SENDSTS_FAILED()
    DeadTarget = SENDSTS_DEADTARGET()

