from thespian.actors import ActorSystem
import sys
ActorSystem((sys.argv + ['multiprocTCPBase'])[1]).shutdown()
