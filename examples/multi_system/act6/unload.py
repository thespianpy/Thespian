from thespian.actors import ActorSystem, Actor, ValidateSource, ValidatedSource
import sys

portnum = int(sys.argv[1])
srchash = sys.argv[2]

asys = ActorSystem('multiprocTCPBase', {'Admin Port': portnum})
asys.unloadActorSource(srchash)
