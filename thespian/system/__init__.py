# This module contains the various ActorSystemBase implementations
# upon which the ActorSystem operates.

from thespian.actors import ActorAddress, ActorSystemMessage, PoisonMessage
from thespian.system.addressManager import *
from thespian.system.messages.status import *
from thespian.system.messages.convention import *

def isInternalActorSystemMessage(msg):
    if isinstance(msg, PoisonMessage):
        msg = msg.poisonMessage
    return isinstance(msg, ActorSystemMessage) and \
        not isinstance(msg, (Thespian_SystemStatus,
                             Thespian_ActorStatus,
                             PoisonMessage))
