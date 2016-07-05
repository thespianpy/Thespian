from thespian.actors import ActorSystem
import sys

portnum = int(sys.argv[1])
addrmv = sys.argv[2]
assert addrmv in '+-'
cap = sys.argv[3]

asys = ActorSystem('multiprocTCPBase', {'Admin Port': portnum})
asys.updateCapability(cap, True if addrmv == '+' else None)
