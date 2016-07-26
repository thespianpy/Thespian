import logging
import time, datetime
from thespian.actors import *
from thespian.test import *
import signal
import os


class KillMeActor(Actor):
    def receiveMessage(self, msg, sender):
        logging.info('EchoActor got %s (%s) from %s', msg, type(msg), sender)
        self.send(sender, os.getpid())


class ParentActor(Actor):
    def receiveMessage(self, msg, sender):
        if msg == 'child':
            self.send(self.createActor(ChildActor), sender)
        if msg == 'hello':
            if not hasattr(self, 'rspmsg'):
                self.rspmsg = 'world'
            self.send(sender, self.rspmsg)
        if isinstance(msg, ChildActorExited):
            self.rspmsg = 'goodbye'

class ChildActor(Actor):
    def receiveMessage(self, msg, sender):
        if isinstance(msg, ActorAddress):
            self.send(msg, os.getpid())


smallwait = datetime.timedelta(milliseconds=350)


@pytest.fixture(params=[
    # Non-deadly
    'signal.SIGCONT-world',
    # 'signal.SIGINT-world',
    # 'signal.SIGUSR1-world',
    # 'signal.SIGUSR2-world',
    # 'signal.SIGHUP-world',
    # # Deadly
    # 'signal.SIGTERM-goodbye',
    # 'signal.SIGQUIT-goodbye',
    # 'signal.SIGABRT-goodbye',
    # 'signal.SIGKILL-goodbye',
])
def testSignal(request):
    return request.param


class TestFuncSafeActorSignals(object):

    def testCreateActorSystem(self, asys):
        pass

    def testSimpleActor(self, asys):
        killme = asys.createActor(KillMeActor)

    def testSigCont(self, asys):
        killme = asys.createActor(KillMeActor)
        killme_pid  = asys.ask(killme, 'pid?', smallwait)
        assert killme_pid  # not 0 or None
        os.kill(killme_pid, signal.SIGCONT)
        assert killme_pid == asys.ask(killme, 'pid again?', smallwait)

    def testChildSigCont(self, asys):
        parent = asys.createActor(ParentActor)
        assert 'world' == asys.ask(parent, 'hello', smallwait)
        child_pid  = asys.ask(parent, 'child', smallwait*3)
        assert child_pid  # not 0 or None
        os.kill(child_pid, signal.SIGCONT)
        assert 'world' == asys.ask(parent, 'hello', smallwait)



# n.b. Cannot test unsafe signals with the simple actor system because
# the signals affect the testing process; often causing it to exit.

class TestFuncMultiProcActorSignals(object):

    def test_signal(self, asys, testSignal):
        if 'proc' not in asys.base_name:
            pytest.skip('Cannot send signals to primary testing process')
        signame, response = testSignal.split('-')
        killme = asys.createActor(KillMeActor)
        killme_pid  = asys.ask(killme, 'pid?', smallwait)
        assert killme_pid  # not 0 or None
        os.kill(killme_pid, eval(signame))
        time.sleep(0.2) # allow signal to be delivered
        r = asys.ask(killme, 'pid again?', smallwait)
        assert (killme_pid if response == 'world' else None) == r


    def testChildSig(self, testSignal, asys):
        if 'proc' not in asys.base_name:
            pytest.skip('Cannot send signals to primary testing process')
        signame, response = testSignal.split('-')
        parent = asys.createActor(ParentActor)
        assert 'world' == asys.ask(parent, 'hello', smallwait)
        child_pid  = asys.ask(parent, 'child', smallwait*3)
        assert child_pid  # not 0 or None
        os.kill(child_pid, eval(signame))
        # Child is not killed and continues running
        time.sleep(0.02) # allow signal to be delivered
        assert response == asys.ask(parent, 'hello', smallwait)
