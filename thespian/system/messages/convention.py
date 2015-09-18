"Messages exchanged between ActorSystems that are part of a Convention"

from thespian.actors import ActorSystemMessage

# ----------------------------------------------------------------------
# ActorSystem interaction
#
# Multiple ActorSystems can interact in a Convention.  All
# ActorSystems that can participate in a Convention must support the
# following message exchanges between themselves.


class ConventionRegister(ActorSystemMessage):
    "Message sent between ActorSystems to join the systems together in a Convention."
    def __init__(self, adminAddress, capabilities, firstTime=False):
        '''firstTime: this is my first registration: I am a new remote system.
                      Anything you thought you knew about me might be wrong.'''
        self.adminAddress = adminAddress
        self.capabilities = capabilities
        self.firstTime = firstTime


class ConventionDeRegister(ActorSystemMessage):
    "Message sent between ActorSystems to exit a previously joined Convention."
    def __init__(self, adminAddress):
        self.adminAddress = adminAddress


class NotifyOnSystemRegistration(ActorSystemMessage):
    """Message sent to the Admin to register or de-register the specified
       address for ActorSystem Convention Registration handling."""
    def __init__(self, handlerAddr, enableNotification):
        self.handlerAddr        = handlerAddr
        self.enableNotification = enableNotification


class SourceHashTransferRequest(ActorSystemMessage):
    """Sent by an ActorSystem that has received a PendingActor create request with a
       sourceHash not currently known by that ActorSystem.  This is sent to the
       requesting ActorSystem which should reply with a SourceHashTransferReply
       message."""
    def __init__(self, sourceHash):
        self.sourceHash = sourceHash


class SourceHashTransferReply(ActorSystemMessage):
    """Response to the SourceHashTransferRequest, containing either the
       sourceData associated with the sourceHash or an error
       indication if the sourceHash is unknown.  A sourceData response
       has a simple fletcher32 checksum to provide a basic integrity
       check on the receiving end."""
    def __init__(self, sourceHash, sourceData=None):
        self.sourceHash = sourceHash
        self.sourceData = sourceData # None/False indicates not-found
        if sourceData:
            self.sourceSum = self._fletcher32(sourceData)
    @staticmethod
    def _fletcher32(sourceData):
        sum1, sum0 = 0xffff, 0xffff
        for x in range(0, len(sourceData), 359):
            for char in sourceData[x: x+359]:
                sum1 += ord(char) if isinstance(char, str) else char
                sum0 += sum1
            sum1 = (sum1 & 0xffff) + (sum1 >> 16)
            sum0 = (sum0 & 0xffff) + (sum0 >> 16)
        sum1 = (sum1 & 0xffff) + (sum1 >> 16)
        sum0 = (sum0 & 0xffff) + (sum0 >> 16)
        return (sum0 << 16) + sum1
    def isValid(self):
        return self.sourceData and self._fletcher32(self.sourceData) == self.sourceSum
