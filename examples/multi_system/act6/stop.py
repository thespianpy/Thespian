from thespian.actors import ActorSystem
import sys

if __name__ == "__main__":
    for portnum in map(int, sys.argv[1:]):
        ActorSystem('multiprocTCPBase', {'Admin Port':portnum}).shutdown()
