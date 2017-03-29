from thespian.actors import *
from thespian.test import *

# Reproduction of problem scenario reported on the thespianpy mailing
# list by Daniel Mitterdorfer, 2017-Mar-01.

class StartLoadGenerator(object): pass


class LoadGenerator(ActorTypeDispatcher):
    def receiveMsg_StartLoadGenerator(self, startmsg, sender):
        self.send(sender, 'started')


class Controller(ActorTypeDispatcher):
    def receiveMsg_int(self, client_count, sender):
        self.generators = []
        for client_id in range(client_count):
            self.generators.append(
                self.createActor(LoadGenerator,
                                 globalName="/rally/driver/worker/%s" % str(client_id),
                                 targetActorRequirements={"coordinator": True}))

        for client_id, generator in enumerate(self.generators):
            self.send(generator, StartLoadGenerator())

        self.responses = [None] * len(self.generators)
        self.requester = sender

    def receiveMsg_str(self, strmsg, sender):
        if strmsg == 'started':
            for idx, each in enumerate(self.generators):
                if each == sender:
                    self.responses[idx] = strmsg
                    if None not in self.responses:
                        self.send(self.requester, 'done')


def test_generators(asys):
    r = asys.ask(asys.createActor(Controller), 8, 3)
    assert r == 'done'
