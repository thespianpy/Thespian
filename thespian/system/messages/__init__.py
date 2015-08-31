"""This module contains messages that are exchanged between Actor
Systems or between the Admin and the individual Actor management."""

from thespian.actors import ActorSystemMessage


class TellMessage(ActorSystemMessage):
    """Wrapper indicating this was a systemBase tell and that no response
       should be generated (e.g. PoisonMessage)."""

    def __init__(self, actualMessage):
        self.actualMessage = actualMessage

    def __str__(self):
        return 'TellMessage(%s)'%str(self.actualMessage)

