from thespian.actors import ActorSystem, Actor, ValidateSource, ValidatedSource
import sys

portnum = int(sys.argv[1])

capability_names = (sys.argv + [''])[2].split(',')
capabilities = dict([('Admin Port', portnum),
                     ('Convention Address.IPv4', ('', 1900)),
                    ] +
                    list(zip(capability_names, [True] * len(capability_names))))


class SimpleSourceAuthority(Actor):
    def receiveMessage(self, msg, sender):
        if msg is True:
            self.registerSourceAuthority()
        if isinstance(msg, ValidateSource):
            self.send(sender,
                      ValidatedSource(msg.sourceHash,
                                      msg.sourceData,
                                      # Thespian pre 3.2.0 has no sourceInfo
                                      getattr(msg, 'sourceInfo', None)))


asys = ActorSystem('multiprocTCPBase', capabilities)
sa = asys.createActor(SimpleSourceAuthority)
asys.tell(sa, True)  # cause source authority to register itself as such

