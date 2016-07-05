from thespian.actors import *


class EncodeThis(object):
    """This is the message requesting an encoding and noting the original
       encoding requester."""
    def __init__(self, rawstr, requester):
        self.rawstr = rawstr
        self.requester = requester


class Encoded(object):
    """This is a message sent to the analyzer.  It holds the original
       encode request information and the encoded version.
    """
    def __init__(self, encode_request, encoding_method, encoded_str):
        self.encode_request  = encode_request
        self.encoding_method = encoding_method
        self.encoded_output  = encoded_str


class Encoder(ActorTypeDispatcher):
    """This is the base encoder class.  This is a functional encoder
       itself, but it simply passes the original message through
       untouched.
    """
    def receiveMsg_tuple(self, message, sender):
        assert message[0] == 'Analyzer'
        self.analyzer = message[1]

    def receiveMsg_EncodeThis(self, encode_request, sender):
        self.send(self.analyzer,
                  Encoded(encode_request,
                          self.__class__.__name__,
                          self.encode(encode_request.rawstr)))

    def encode(self, rawstr):
        "Override this method to change the encoding"
        return rawstr


class Base64Encoder(Encoder):
    def encode(self, rawstr):
        from base64 import b64encode
        return b64encode(rawstr.encode('ascii'))


class Rot13Encoder(Encoder):
    def encode(self, rawstr):
        return ''.join([chr(self.rot13(ord(C))) for C in rawstr])
    @staticmethod
    def rot13(val):
        if val >= ord('a') and val <= ord('z'):
            return ((val + 13) % 26) + ord('a')
        if val >= ord('A') and val <= ord('Z'):
            return ((val + 13) % 26) + ord('A')
        return val


