try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    rangefun = xrange
except NameError:
    rangefun = range


toSendBuffer = lambda A, ser=pickle.dumps: (lambda AP: ('%d>'%len(AP)).encode('utf-8') + AP)(ser(A))


class ReceiveBuffer(object):
    def __init__(self, serializer=pickle.loads):
        self._buf         = b''
        self._blen        = 0
        self._size        = None
        self._extra       = b''
        self._deserialize = serializer
    def addMore(self, buf):
        "Called to add additional received data to this in-progress packet buffer."
        if self._size is not None:
            blen = len(buf)
            if self._blen + blen <= self._size:
                self._buf += buf
                self._blen += blen
            elif self._blen == self._size:
                self._extra += buf
            else:
                want = self._size - self._blen
                self._buf += buf[:want]
                self._extra = buf[want:]
        else:
            rem = ''
            if type(buf) == type(''):
                # Python2: buf is str()
                markPos = buf.find('>')
                if markPos >= 0:
                    rem = bytes(buf[markPos+1:])
            else:
                # Python3: buf is bytes()
                markPos = buf.find(ord('>'))
                if markPos >= 0:
                    rem = buf[markPos+1:]
            if markPos == -1:
                self._buf += buf
                self._blen = 1  # unimportant if _size not set, but non-zero for is_empty
            else:
                try:
                    self._size = int(self._buf + buf[:markPos])
                except ValueError:
                    thesplog('Cannot determine stream buffer size from %s + %s',
                             self._buf, buf, markPos, level=logging.ERROR)
                    raise
                self._buf = rem[:self._size]
                self._blen = len(self._buf)
                if len(rem) > self._size:
                    self._extra = rem[self._size:]
    def is_empty(self):
        # Does not indicate there is any recoverable buffer, just that
        # there has been some input received.
        return self._blen == 0 and self._size is None
    def remainingAmount(self):
        "Specifies the amount still to read to obtain the packet"
        if self._size is None:
            return 20
        return self._size - len(self._buf)
    def isDone(self):
        "Returns true if no more data should be read from the socket."
        # might be true if no size could be reasonably read from the data so-far
        return (self._size is not None and self._blen == self._size) or \
            (self._size is None and self._blen > 40)  # corrupted packet
    def removeExtra(self):
        self._extra = b''
    def completed(self):
        "Returns the packet and any extra data if fully read, otherwise None"
        if self._size is not None and self._blen == self._size:
            return self._deserialize(self._buf), self._extra
        return None


def isControlMessage(msg):
    return msg in [ackPacket, ackDataErrPacket]


ackPacket = 'ACK'
ackDataErrPacket = 'ACK+DATAERR'
ackMsg = toSendBuffer(ackPacket)
ackDataErrMsg = toSendBuffer(ackDataErrPacket)
