from thespian.actors import *

class LowerFloor(Actor):
    def receiveMessage(self, msg, sender):
        if type(msg) == type(""):
            self.send(sender, 'Heard: ' + str(msg))
        elif type(msg) == type( (1,2) ):
            self.send(msg[1], 'And Heard: ' + str(msg[0]))
