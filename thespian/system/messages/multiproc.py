from thespian.actors import ActorSystemMessage

class EndpointConnected(ActorSystemMessage):
    # internal message sent from child to parent on startup connection
    def __init__(self, childInstance):
        self.childInstance = childInstance


class LoggerConnected(ActorSystemMessage): pass
