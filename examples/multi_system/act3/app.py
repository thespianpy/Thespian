from thespian.actors import *
from datetime import timedelta
from encoder import EncodeThis, Encoded, Encoder, Base64Encoder, Rot13Encoder
from morse import MorseEncoder


class Acceptor(ActorTypeDispatcher):

    def __init__(self, *args, **kw):
        super(Acceptor, self).__init__(*args, **kw)
        # The analyzer and encoders will be Actor Addresses; these
        # cannot be created now because the Acceptor has not been
        # fully initialized into the ActorSystem until after the
        # __init__ has finished.
        self.analyzer = None
        self.encoders = []

    def receiveMsg_str(self, message, sender):
        """Primary entry point for receiving strings that are to be encoded
           and analyzed.
        """
        # First, make sure the analyzer and encoders are present
        if not self.analyzer:
            self.analyzer = self.createActor(Analyzer)
            self.encoders = [ self.createActor(Encoder),
                              self.createActor(Base64Encoder),
                              self.createActor(MorseEncoder),
                              self.createActor(Rot13Encoder),
                            ]

        # Now send the input string to each encoder.  The encoders
        # already have the analyzer address to forward the result
        # to, but include the original sender's address so that
        # the analyzer knows where to send the response.

        for each in self.encoders:
            self.send(each,
                      EncodeThis(message, sender, self.analyzer))


class Analyzer(ActorTypeDispatcher):
    def receiveMsg_Encoded(self, encoded, sender):
        density = (len(encoded.encode_request.rawstr) * 1.0) / len(encoded.encoded_output)
        self.send(encoded.encode_request.requester,
                  '%s [density=%0.3f]  %s'%(encoded.encoding_method,
                                            density,
                                            encoded.encoded_output))


if __name__ == "__main__":
    import sys
    asys = ActorSystem((sys.argv + ['multiprocTCPBase'])[1])

    # Note: the following doesn't work because actor is created by
    # admin, started by the start.py, and the Acceptor class is not
    # part of start.py
    #     app = asys.createActor(Acceptor)

    app = asys.createActor('app.Acceptor',
                           globalName='Acceptor')
    r = asys.ask(app, sys.stdin.read().strip(), timedelta(seconds=1))
    while r:
        print(r)
        r = asys.listen(timedelta(seconds=0.5))
    sys.exit(0)
