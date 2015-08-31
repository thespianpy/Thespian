from thespian.actors import *

class UpperFloor(Actor):
    def receiveMessage(self, msg, sender):
        if type(msg) == type(""):
            self.send(sender, 'Viewed: ' + str(msg))
        elif type(msg) == type((1,2)):
            if not hasattr(self, 'subA'):
                self.subA = self.createActor('thespian.test.sub1.sub2.lower.LowerFloor')
            self.send(self.subA, (msg[1], sender))

