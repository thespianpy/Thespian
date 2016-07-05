import logging
import time, datetime
from thespian.actors import *
from thespian.test import *

class Larry(Actor):
    def receiveMessage(self, msg, sender):
        if msg != 'Silence!':
            self.send(sender, 'Hey!')


class Mo(Actor):
    def receiveMessage(self, msg, sender):
        pass

class Curly(Actor):
    def receiveMessage(self, msg, sender):
        logging.debug('Sending msg1')
        self.send(sender, 'Wise guy, eh?')
        logging.debug('Sending msg2')
        self.send(sender, 'Pow!')


class TestFuncActorSysAPI(object):
    responseDelay = 0.60

    def testTell(self, asys):
        mo = asys.createActor(Mo)
        asys.tell(mo, 'hello')
        asys.tell(mo, 'goodbye')

    def testAsk(self, asys):
        larry = asys.createActor(Larry)
        rsp = asys.ask(larry, 'hello', self.responseDelay)
        assert rsp == 'Hey!'
        rsp = asys.ask(larry, 'Silence!', self.responseDelay)
        assert rsp is None

    def testListen(self, asys):
        curly = asys.createActor(Curly)
        rsp = asys.ask(curly, 'hello', self.responseDelay)
        assert rsp, 'Wise guy == eh?'
        rsp = asys.listen(self.responseDelay)
        assert rsp == 'Pow!'

    def testAskIsTellPlusListen(self, asys):
        larry = asys.createActor(Larry)
        rsp = asys.ask(larry, 'hello', self.responseDelay)
        assert rsp == 'Hey!'

        rsp = asys.listen(self.responseDelay)
        assert rsp is None

        asys.tell(larry, 'hello')
        rsp = asys.listen(self.responseDelay)
        assert rsp == 'Hey!'

        rsp = asys.listen(self.responseDelay)
        assert rsp is None

    def testResponsesFromAnywhere(self, asys):
        aS = asys
        larry = aS.createActor(Larry)
        mo    = aS.createActor(Mo)
        curly = aS.createActor(Curly)

        aS.tell(curly, 'hello')
        aS.tell(mo, 'hello')
        rsp = aS.ask(larry, 'hello', self.responseDelay)

        responses = [ 'Wise guy, eh?', 'Pow!', 'Hey!' ]
        while responses:
            assert rsp is not None
            assert rsp in responses
            responses = [R for R in responses if R != rsp]
            rsp = asys.listen(self.responseDelay)
