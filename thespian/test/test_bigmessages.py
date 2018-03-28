import logging
import time, datetime
from thespian.test import *
from thespian.actors import *

askTimeout = datetime.timedelta(seconds=5)

class Whale(Actor):
    def receiveMessage(self, msg, sender):
        if isinstance(msg, tuple):
            self.send(sender, msg[1] * msg[0])


class Shrimp(Actor):
    def receiveMessage(self, msg, sender):
        if isinstance(msg, tuple):
            self.send(sender, msg[1])


class TestFuncSimpleActorOperations(object):
    def testCreateActorSystem(self, asys):
        pass

    def testSimpleActor(self, asys):
        whale = asys.createActor(Whale)
        shrimp = asys.createActor(Shrimp)

    def testSimpleActorAskOneHello(self, asys):
        whale = asys.createActor(Whale)
        shrimp = asys.createActor(Shrimp)
        testdata = (1, 'hello')
        r = asys.ask(shrimp, testdata, askTimeout)
        assert r == 'hello'
        r = asys.ask(whale, testdata, askTimeout)
        assert r == 'hello' * 1

    def testSimpleActorAskFiveHello(self, asys):
        whale = asys.createActor(Whale)
        shrimp = asys.createActor(Shrimp)
        testdata = (5, 'hello')
        r = asys.ask(shrimp, testdata, askTimeout)
        assert r == 'hello'
        r = asys.ask(whale, testdata, askTimeout)
        assert r == 'hellohellohellohellohello'
        assert r == 'hello' * 5

    def testSimpleActorAsk50K(self, asys):
        whale = asys.createActor(Whale)
        shrimp = asys.createActor(Shrimp)
        testdata = (10*1024, 'hello')
        r = asys.ask(shrimp, testdata, askTimeout)
        assert r == 'hello'
        r = asys.ask(whale, testdata, askTimeout)
        assert r == 'hello' * 10 * 1024

    def testSimpleActorAsk500K(self, asys):
        actor_system_unsupported(asys, 'multiprocUDPBase')
        whale = asys.createActor(Whale)
        shrimp = asys.createActor(Shrimp)
        testdata = (100*1024, 'hello')
        r = asys.ask(shrimp, testdata, askTimeout)
        assert r == 'hello'
        r = asys.ask(whale, testdata, askTimeout)
        assert r == 'hello' * 100 * 1024

    def testSimpleActorAsk5M(self, asys):
        actor_system_unsupported(asys, 'multiprocUDPBase')
        whale = asys.createActor(Whale)
        shrimp = asys.createActor(Shrimp)
        testdata = (1024*1024, 'hello')
        r = asys.ask(shrimp, testdata, askTimeout)
        assert r == 'hello'
        r = asys.ask(whale, testdata, askTimeout)
        assert r == 'hello' * 1024 * 1024

    def testSimpleActorAsk10M(self, asys):
        actor_system_unsupported(asys, 'multiprocUDPBase')
        whale = asys.createActor(Whale)
        shrimp = asys.createActor(Shrimp)
        testdata = (2*1024*1024, 'hello')
        r = asys.ask(shrimp, testdata, askTimeout)
        assert r == 'hello'
        r = asys.ask(whale, testdata, askTimeout * 2)
        assert r == 'hello' * 2 * 1024 * 1024

    def testSimpleActorAsk20M(self, asys):
        actor_system_unsupported(asys, 'multiprocUDPBase')
        whale = asys.createActor(Whale)
        shrimp = asys.createActor(Shrimp)
        testdata = (4*1024*1024, 'hello')
        r = asys.ask(shrimp, testdata, askTimeout)
        assert r == 'hello'
        r = asys.ask(whale, testdata, askTimeout * 2)
        assert r == 'hello' * 4 * 1024 * 1024

    def testSimpleActorAsk25M(self, asys):
        actor_system_unsupported(asys, 'multiprocUDPBase')
        whale = asys.createActor(Whale)
        shrimp = asys.createActor(Shrimp)
        testdata = (5*1024*1024, 'hello')
        r = asys.ask(shrimp, testdata, askTimeout)
        assert r == 'hello'
        r = asys.ask(whale, testdata, askTimeout * 2)
        assert r == 'hello' * 5 * 1024 * 1024

    def testSimpleActorAsk50M(self, asys):
        actor_system_unsupported(asys, 'multiprocUDPBase')
        whale = asys.createActor(Whale)
        shrimp = asys.createActor(Shrimp)
        testdata = (10*1024*1024, 'hello')
        r = asys.ask(shrimp, testdata, askTimeout)
        assert r == 'hello'
        r = asys.ask(whale, testdata, askTimeout * 4)
        assert r == 'hello' * 10 * 1024 * 1024


if __name__ == "__main__":
    message = 'helloworld'
    fmt = 'Expected size = %d (%.2f MiB),  receive size = %s %s,  elapsed = %s, throughput = %.2f bytes/s (%.2f MiB/s)'
    asys = ActorSystem('multiprocTCPBase')
    try:
        whale = asys.createActor(Whale)
        for scale in [ 1, 10,
                       10 * 1024,
                       100 * 1024,
                       1024 * 1024,
                       2 * 1024 * 1024,
                       4 * 1024 * 1024,
                       5 * 1024 * 1024 ]:
            testdata = (scale, message)
            max_delay = datetime.timedelta(seconds = 2,  # minimum
                                           microseconds=scale * 20)
            tstart = datetime.datetime.now()
            r = asys.ask(whale, testdata, max_delay)
            tend = datetime.datetime.now()
            elapsed = tend - tstart
            bytesrx = len(r) if r else 0
            bytesttl = len(message) * 1.0 + bytesrx
            print(fmt % (scale * len(message),
                         scale * 1.0 * len(message) / 1024 / 1024,
                         len(r) if r else str(r),
                         'ok' if bytesrx == scale * len(message)
                         else 'MISMATCH',
                         str(elapsed),
                         bytesttl / elapsed.total_seconds(),
                         bytesttl / 1024 / 1024 / elapsed.total_seconds()))
    finally:
        asys.shutdown()
