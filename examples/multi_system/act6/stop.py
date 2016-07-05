from thespian.actors import ActorSystem
import sys
for portnum in map(int, sys.argv[1:]):
    ActorSystem('multiprocTCPBase', {'Admin Port':portnum}).shutdown()
