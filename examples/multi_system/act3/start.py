from thespian.actors import ActorSystem
import sys

if __name__ == "__main__":
    ActorSystem((sys.argv + ['multiprocTCPBase'])[1])
