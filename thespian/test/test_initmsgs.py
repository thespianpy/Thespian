from time import sleep
from datetime import timedelta
from thespian.test import *
from thespian.actors import *
from thespian.transient import transient, transient_idle
from thespian.initmsgs import initializing_messages
from thespian.troupe import troupe

max_ask_wait = timedelta(milliseconds=250)

class Msg1(object): pass
class Msg2(object): pass
class Msg3(object): pass
class Msg4(object): pass


@initializing_messages([('i_msg1', Msg1, True),
                        ('i_msg2', Msg2),
                        ('i_msg3', str),
                       ], 'init_done')
class Actor1(ActorTypeDispatcher):
    def init_done(self):
        self.send(self.i_msg1_sender, 'init is done')
    def receiveMsg_str(self, strmsg, sender):
        self.send(self.i_msg1_sender, 's:'+strmsg)
    def receiveMsg_Msg3(self, msg3, sender):
        self.send(sender, self.i_msg2)
    def receiveMsg_Msg4(self, msg4, sender):
        self.send(self.i_msg1_sender, msg4)
        self.send(sender, self.i_msg3)

@initializing_messages([('proxy', ActorAddress, True)])
class ProxyActor(Actor):
    def receiveMessage(self, msg, sender):
        if not isinstance(msg, ActorSystemMessage):
            if sender == self.proxy_sender:
                self.send(self.proxy, msg)
            else:
                self.send(self.proxy_sender, msg)

def test_simpleinit(asys):
    t1 = asys.createActor(ProxyActor)
    asys.tell(t1, asys.createActor(Actor1))

    asys.tell(t1, Msg1())
    r = asys.ask(t1, "ready?", max_ask_wait)
    assert r is None

    r = asys.ask(t1, Msg2(), max_ask_wait)
    assert r == "init is done"

    r = asys.ask(t1, "running?", max_ask_wait)
    assert r == "s:running?"

    m4 = Msg4()
    r = asys.ask(t1, m4, max_ask_wait)
    r2 = asys.listen(max_ask_wait)

    if r == "ready?":
        assert r == "ready?"
        assert isinstance(r2, Msg4)
    else:
        assert isinstance(r, Msg4)
        assert r2 == "ready?"

    asys.shutdown()


@initializing_messages([('i_msg1', Msg1, True),
                        ('i_msg2', Msg2),
                        ('i_msg3', str),
                       ], 'init_done')
@transient(timedelta(seconds=1))
class Actor2(ActorTypeDispatcher):
    def init_done(self):
        self.send(self.i_msg1_sender, self.i_msg3)
    def receiveMsg_Msg3(self, msg3, sender):
        self.send(sender, self.i_msg2)

def test_init_transient(asys):
    t1 = asys.createActor(ProxyActor)
    asys.tell(t1, asys.createActor(Actor2))

    asys.tell(t1, Msg1())
    r = asys.ask(t1, "ready?", max_ask_wait)
    assert r is None

    r = asys.ask(t1, Msg2(), max_ask_wait)
    assert r == "ready?"

    r = asys.ask(t1, Msg3(), max_ask_wait)
    assert isinstance(r, Msg2)

    asys.tell(t1, Msg3())
    r = asys.ask(t1, Msg3(), max_ask_wait)
    assert isinstance(r, Msg2)
    r = asys.listen(max_ask_wait)
    assert isinstance(r, Msg2)
    r = asys.listen(max_ask_wait)
    assert r is None

    r = asys.ask(t1, Msg3(), max_ask_wait)
    assert isinstance(r, Msg2)

    # n.b. if the system is slow such that it takes more than 1 second
    # to reach this point, this test will have a false failure.
    sleep(1.1)

    r = asys.ask(t1, Msg3(), max_ask_wait)
    assert r is None
