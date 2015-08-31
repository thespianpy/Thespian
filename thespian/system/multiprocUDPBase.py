'''The multiprocUDPBase runs each Actor in its own separate process
for a truly asynchronous system.  The default Actor communication
method is by utilizing UDP messages.

The multiprocUDP SystemBase is shared between all processes that
utilize the same Administrator coordinates, and can be used for
inter-system communications.  The lifetime of the Actor System
exceeds that of the process that started it and persists until an
explicit shutdown.

'''

from thespian.system.systemBase import systemBase
from thespian.system.utilis import thesplog
from thespian.system.multiprocCommon import multiprocessCommon
from thespian.system.transport.UDPTransport import UDPTransport


class ActorSystemBase(multiprocessCommon):

    transportType = UDPTransport

    def __init__(self, system, logDefs = None):
        system.capabilities['Thespian ActorSystem Name'] = 'multiprocUDPBase'
        system.capabilities['Thespian ActorSystem Version'] = 1
        super(ActorSystemBase, self).__init__(system, logDefs)
