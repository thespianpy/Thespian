import logging
from thespian.actors import ActorExitRequest
from thespian.system.admin.adminCore import AdminCore
from thespian.system.transport import TransmitIntent
from thespian.system.messages.admin import PendingActorResponse


class GlobalNamesAdmin(AdminCore):
    "Extends the AdminCore with management of globally-per-host-registered named Actors."

    def __init__(self, *args, **kw):
        super(GlobalNamesAdmin, self).__init__(*args, **kw)
        self._globalNames = {}  # key=name, value=ActorAddress


    def h_PendingActor(self, envelope):
        gName = envelope.message.globalName
        if not gName:
            return super(GlobalNamesAdmin, self).h_PendingActor(envelope)

        if gName in self._globalNames:
            # Actor already registered with this name... here it is.
            self._send_intent(
                TransmitIntent(envelope.sender,
                               PendingActorResponse(envelope.message.forActor,
                                                    envelope.message.instanceNum,
                                                    gName,
                                                    actualAddress = self._globalNames[gName])))
            return True

        return super(GlobalNamesAdmin, self).h_PendingActor(envelope)


    def _pendingActorReady(self, childInstance, actualAddress):
        if childInstance in self._pendingChildren:
            gName = self._pendingChildren[childInstance].message.globalName
            if gName:
                if gName in self._globalNames:
                    # This is the loser of the race... just kill it.
                    self._send_intent(
                        TransmitIntent(actualAddress, ActorExitRequest(recursive=True)))
                    actualAddress = self._globalNames[gName]
                else:
                    self._globalNames[gName] = actualAddress
        return super(GlobalNamesAdmin, self)._pendingActorReady(childInstance,
                                                                actualAddress)


    def h_ChildActorExited(self, envelope):
        for gName in self._globalNames:
            if self._globalNames[gName] == envelope.message.childAddress:
                del self._globalNames[gName]
                break
        return super(GlobalNamesAdmin, self).h_ChildActorExited(envelope)


    def _updateStatusResponse(self, resp):
        for gName,gAddr in self._globalNames.items():
            resp.addGlobalActor(gName, gAddr)
        super(GlobalNamesAdmin, self)._updateStatusResponse(resp)
