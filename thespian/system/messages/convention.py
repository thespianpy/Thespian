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
    def __init__(self, adminAddress, capabilities, firstTime=False, preRegister=False):
        '''firstTime: this is my first registration: I am a new remote system.
                      Anything you thought you knew about me might be wrong.'''
        self.adminAddress = adminAddress
        self.capabilities = capabilities
        self.firstTime = firstTime
        self.preRegister = preRegister  # n.b. added in 2.5.0; use getattr
    def __str__(self):
        return 'ConventionRegister(adminAddress=%(adminAddress)s' \
            ', firstTime=%(firstTime)s' \
            ', preRegister=%(preRegister)s' \
            ', capabilities=%(capabilities)s' \
            ')' % self.__dict__

    def __eq__(self, o):
        return self.adminAddress == o.adminAddress and \
            self.firstTime == o.firstTime and \
            self.preRegister == o.preRegister and \
            self.capabilities == o.capabilities

    def __ne__(self, o):
        return not self.__eq__(o)


class ConventionDeRegister(ActorSystemMessage):
    "Message sent between ActorSystems to exit a previously joined Convention."
    def __init__(self, adminAddress, preRegistered=False):
        self.adminAddress = adminAddress
        self.preRegistered = preRegistered  # n.b. added in 2.5.0; use getattr

    def __str__(self):
        return 'ConventionDeRegister(adminAddress=%(adminAddress)s' \
            ', preRegistered=%(preRegistered)s' \
            ')' % self.__dict__

    def __eq__(self, o):
        return self.adminAddress == o.adminAddress and \
            self.preRegistered == o.preRegistered

    def __ne__(self, o):
        return not self.__eq__(o)


class ConventionInvite(ActorSystemMessage):
    """Message sent periodically to preRegistered remote systems inviting
       them to send a ConventionRegister message back."""
    pass

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

    def __init__(self, sourceHash, have_local_authority=False):
        self.sourceHash = sourceHash
        self.prefer_original = have_local_authority


class SourceHashTransferReply(ActorSystemMessage):
    """Response to the SourceHashTransferRequest, containing either the
       sourceData associated with the sourceHash or an error
       indication if the sourceHash is unknown.  A sourceData response
       has a simple fletcher32 checksum to provide a basic integrity
       check on the receiving end."""

    def __init__(self, sourceHash, sourceData=None, sourceInfo=None,
                 original_form=False):
        self.sourceHash = sourceHash
        self.sourceInfo = sourceInfo
        self.sourceData = sourceData # None/False indicates not-found
        self.original_form = original_form
        if sourceData and not original_form:
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
        if not self.sourceData:
            return False
        src_sum = getattr(self, 'sourceSum', None)
        if src_sum:
            return self._fletcher32(self.sourceData) == src_sum
        return True
