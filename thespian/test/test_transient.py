from time import sleep
from datetime import timedelta
from thespian.test import *
from thespian.actors import *
from thespian.transient import transient, transient_idle


max_ask_wait    = timedelta(milliseconds=250)


class TBase(ActorTypeDispatcher):
    def receiveMsg_str(self, strmsg, sender):
        self.send(sender, "GOT: " + strmsg)


@transient(timedelta(seconds=1))
class T1(TBase): pass


@transient_idle(timedelta(seconds=1))
class T2(TBase): pass


class NotTransient(TBase): pass


def test_transient(asys):
    t1 = asys.createActor(T1)
    t2 = asys.createActor(T2)
    nt = asys.createActor(NotTransient)

    # Actors do not become transient until they receive a message, so
    # they should not be affected by the initial sleep
    sleep(2)

    r = asys.ask(t1, 'hi.1', max_ask_wait)
    assert r == 'GOT: hi.1'

    r = asys.ask(t2, 'hi.2', max_ask_wait)
    assert r == 'GOT: hi.2'

    r = asys.ask(nt, 'hi.3', max_ask_wait)
    assert r == 'GOT: hi.3'

    # All actors should still be there
    r = asys.ask(t1, 'hi.4', max_ask_wait)
    assert r == 'GOT: hi.4'

    r = asys.ask(t2, 'hi.5', max_ask_wait)
    assert r == 'GOT: hi.5'

    r = asys.ask(nt, 'hi.6', max_ask_wait)
    assert r == 'GOT: hi.6'

    # Now poke the transient idle in a little bit, but let the transient die
    sleep(0.3)
    r = asys.ask(t2, 'hi.7', max_ask_wait)
    assert r == 'GOT: hi.7'

    sleep(0.3)
    r = asys.ask(t2, 'hi.8', max_ask_wait)
    assert r == 'GOT: hi.8'

    sleep(0.3)
    r = asys.ask(t2, 'hi.9', max_ask_wait)
    assert r == 'GOT: hi.9'

    sleep(0.3)
    r = asys.ask(t2, 'hi.10', max_ask_wait)
    assert r == 'GOT: hi.10'

    # Has been enough time for the the transient to die

    # In some bases (e.g. simple), timeouts are detected only when
    # actor system calls are made, and the ActorExitRequest that the
    # transient sends itself may be queued behind the first message.
    # Thus, if the first message is to t1, it may respond to the
    # message before exiting.  This is acceptable functionality, but
    # to keep the tests here simple, a message is first sent to an
    # actor that is not expected to be expired so that expired wakeups
    # and shutdowns are processed as well.

    r = asys.ask(t2, 'hi.11', max_ask_wait)
    assert r == 'GOT: hi.11'

    r = asys.ask(nt, 'hi.12', max_ask_wait)
    assert r == 'GOT: hi.12'

    r = asys.ask(t1, 'hi.13', max_ask_wait)
    assert r == None

    # And let the transient idle be idle long enough to die

    sleep(1.1)

    r = asys.ask(nt, 'hi.14', max_ask_wait)
    assert r == 'GOT: hi.14'

    r = asys.ask(t2, 'hi.15', max_ask_wait)
    assert r == None

    asys.shutdown()
