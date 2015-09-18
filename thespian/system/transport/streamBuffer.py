try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    rangefun = xrange
except NameError:
    rangefun = range


toSendBuffer = lambda A, ser=pickle.dumps: (lambda AP: ('%d>'%len(AP)).encode('utf-8') + AP)(ser(A))


class ReceiveBuffer:
    def __init__(self, serializer=pickle.loads):
        self._buf         = b''
        self._size        = None
        self._extra       = b''
        self._deserialize = serializer
    def addMore(self, buf):
        "Called to add additional received data to this in-progress packet buffer."
        if self._size is not None:
            if len(self._buf) + len(buf) <= self._size:
                self._buf += buf
            else:
                bufRem = self.size - len(self._size)
                self._buf += buf[:rem]
                self._extra = buf[rem:]
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
            else:
                self._size = eval(self._buf + buf[:markPos])
                self._buf = rem[:self._size]
                if len(rem) > self._size:
                    self._extra = rem[self._size:]
    def remainingAmount(self):
        "Specifies the amount still to read to obtain the packet"
        if self._size is None:
            return 20
        return self._size - len(self._buf)
    def isDone(self):
        "Returns true if no more data should be read from the socket."
        # might be true if no size could be reasonably read from the data so-far
        return (self._size is not None and len(self._buf) == self._size) or \
            (self._size is None and len(self._buf) > 40)  # corrupted packet
    def completed(self):
        "Returns the packet and any extra data if fully read, otherwise None"
        if self._size is not None and len(self._buf) == self._size:
            return self._deserialize(self._buf), self._extra
        return None


ackPacket = 'ACK'
ackDataErrPacket = 'ACK+DATAERR'
ackMsg = toSendBuffer(ackPacket)
ackDataErrMsg = toSendBuffer(ackDataErrPacket)
