'''The multiprocQueueBase runs each Actor in its own separate process
for a truly asynchronous system.  The default Actor communication
method is by utilizing multiprocess Queue objects.  The Actors are
constrained to the current system, which is unique to the starting
process. There is no inter-system communications possible, nor sharing
of the Actor System with processes that are siblings of the starting
process (i.e. not started by the ActorSystem itself).

The multiprocQueueBase System Base is used between all processes
that were created from the original creation of the SystemBase, but a
new SystemBase is created by each initiating process and the Queues
are unique between the different SystemBase elements.  The lifetime of
the Actor System is limited to the lifetime of the process that
created it.

The multiprocQueueBase can only be used for communications on the
current system and not for inter-system communications.

'''

# n.b. system lifetime is limited to the creating process lifetime
# because Queues are specific to a process family and cannot be
# extended beyond that (i.e. Queues can only be inherited at Process
# startup time, not passed to other Processes explicitly).


from thespian.system.systemBase import systemBase, thesplog
from thespian.system.multiprocCommon import multiprocessCommon
from thespian.system.transport.MultiprocessQueueTransport import MultiprocessQueueTransport


class ActorSystemBase(multiprocessCommon):

    transportType = MultiprocessQueueTransport

    def __init__(self, system, logDefs = None):
        system.capabilities['Thespian ActorSystem Name'] = 'multiprocQueueBase'
        system.capabilities['Thespian ActorSystem Version'] = 1
        super(ActorSystemBase, self).__init__(system, logDefs)

