"Messages used to internally control thesplog settings."

from thespian.actors import ActorSystemMessage

class SetLogging(ActorSystemMessage):
    def __init__(self, threshold, useLogging, useFile):
        self.threshold  = threshold
        self.useLogging = useLogging
        self.useFile    = useFile

