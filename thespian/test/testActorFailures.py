from thespian.actors import *
from thespian.test import *
import time


class TellChild(object):
    def __init__(self, msg):
        self.msg = msg
class TellSon(TellChild): pass
class TellDaughter(TellChild): pass

class PassedMessage(object):
    def __init__(self, msg, origSender):
        self.msg = msg
        self.origSender = origSender
    # Can't compare these messages to anything either; make sure this
    # doesn't break Thespian internals.
    def __eq__(self, o): raise ValueError
    def __ne__(self, o): raise ValueError


class Parent(Actor):
    def __init__(self, *args, **kw):
        super(Parent, self).__init__(*args, **kw)
        self.son = None      # Always a NonStarter
        self.daughter = None  # Starter on restart
        self.poisonedChild = False

    def receiveMessage(self, msg, sender):
        if isinstance(msg, PassedMessage):
            sender = msg.origSender
            msg = msg.msg

        if msg == 'have a son?':
            if not self.son:
                self.son = self.createActor(NonStarter)
            self.send(sender, self.son if self.son else 'no')

        elif msg == 'have a daughter?':
            if not self.daughter:
                self.daughter = self.createActor(NonStarter)
            self.send(sender, self.daughter if self.daughter else 'no')

        elif isinstance(msg, TellChild):
            self.send(self.son if isinstance(msg, TellSon) else self.daughter,
                      PassedMessage(msg.msg, sender))

        elif msg == 'name?':
            self.send(sender, self.myAddress)

        elif isinstance(msg, Deadly):
            if msg.countdown:
                self.send(self.son or self.daughter, Deadly(msg.countdown - 1))
            else:
                raise ValueError('Deadly value received!')
        elif isinstance(msg, PoisonMessage):
            self.poisonedChild = True
        elif msg == 'poisoned child?':
            self.send(sender, self.poisonedChild)
        elif isinstance(msg, KillReq):
            if msg.countdown:
                self.send(self.son or self.daughter, KillReq(msg.countdown - 1))
            else:
                self.send(self.myAddress, ActorExitRequest())


class RestartParent(Parent):
    """Sons that exit are restarted with NonStarter, all other children
       are restarted as RestartParent.
    """
    def __init__(self, *args, **kw):
        super(RestartParent, self).__init__(*args, **kw)
        self._numCreatesLeft = 5
    def receiveMessage(self, msg, sender):
        if isinstance(msg, ChildActorExited):
            if self._numCreatesLeft:
                self._numCreatesLeft = self._numCreatesLeft - 1
                if msg.childAddress == self.son:
                    self.son = self.createActor(NonStarter)
                elif self.daughter == msg.childAddress:
                    self.daughter = self.createActor(RestartParent)
        else:
            super(RestartParent, self).receiveMessage(msg, sender)


class NoRestartParent(Parent):
    def receiveMessage(self, msg, sender):
        if isinstance(msg, ChildActorExited):
            pass # ignore this... child not restarted
        else:
            super(NoRestartParent, self).receiveMessage(msg, sender)


class NonStarter(Actor):
    def __init__(self, *args, **kw):
        raise Exception('This actor can never start')
    def receiveMessage(self, msg, sender):
        if not isinstance(msg, ActorExitRequest):
            self.send(sender, msg)

class Confused(Actor):
    "Generate exception on ActorExitRequest"
    def __init__(self, *args, **kw):
        self.name = 'dunno'
        super(Confused, self).__init__(*args, **kw)
    def receiveMessage(self, msg, sender):
        if isinstance(msg, (ActorExitRequest, Deadly)):
            raise NameError("Who am I?")
        elif msg == "name?":
            self.send(sender, self.name)
        elif msg == "subactor?":
            self.send(sender, self.createActor(Confused))
        elif isinstance(msg, ChildActorExited):
            self.name = 'permanent'


class Deadly(object):
    def __init__(self, v):
        self.countdown = v

class KillReq(object):
    def __init__(self, v):
        self.countdown = v


# Note: multiprocQueueBase is marked as unstable because that system
# base assumes that Queue.put() messages have been successfully sent;
# this is not true, and especially when the target has already exited.

class TestFuncActorFailures(object):

    def test01_NonStartingSystemLevelActor(self, asys):
        nonstarter = asys.createActor(NonStarter)
        # just finish, make sure no exception is thrown.  Primary
        # actors (those owned by the ActorSystem itself) are not
        # restarted on failure, so the actor won't actually be
        # recreated.  The "anything" message will actually be routed
        # to the Dead Letter handler (see testDeadLettering).
        assert asys.ask(nonstarter, "anything", 0.3) is None

    def test02_NonStartingSubActorWithRestarts(self, asys):
        unstable_test(asys, 'multiprocUDPBase', 'multiprocQueueBase')
        parent = asys.createActor(RestartParent)

        tellParent = lambda m: asys.tell(parent, m)

        askParent = lambda m: asys.ask(parent, m, 2)
        askKid    = lambda m: askParent(TellDaughter(m))

        assert askParent('name?') == parent
        son = askParent('have a son?')
        assert son is not None
        assert askParent(TellSon('name?')) is None

        assert askParent('name?') == parent
        tellParent(ActorExitRequest())
        assert askParent('name?') is None


    def test03_NonStartingSubActorWithoutRestarts(self, asys):
        unstable_test(asys, 'multiprocUDPBase')
        parent = asys.createActor(NoRestartParent)

        tellParent = lambda m: asys.tell(parent, m)

        askParent        = lambda m: asys.ask(parent, m, 0.5)

        assert askParent('name?') == parent
        son = askParent('have a son?')
        assert son is not None  # got an Address back, but Son failed to start
        assert askParent(TellSon('name?')) is None  # dead-lettered, so no response

        assert askParent('name?') == parent
        tellParent(ActorExitRequest())
        assert askParent('name?') is None


    def test04_RestartedSubActorWithRestarts(self, asys):
        unstable_test(asys, 'multiprocUDPBase', 'multiprocQueueBase')
        parent = asys.createActor(RestartParent)

        tellParent = lambda m: asys.tell(parent, m)

        askParent = lambda m: asys.ask(parent, m, 0.5)
        askKid    = lambda m: askParent(TellDaughter(m))

        assert askParent('name?') == parent

        kid = askParent('have a daughter?')
        assert kid is not None
        assert askKid('name?') is not None

        assert askParent('name?') == parent
        assert askKid('name?') is not None

        stableKid = askKid('name?')
        assert asys.ask(stableKid, 'name?', 0.4) == stableKid

        # root Actors are not restarted which should cause children to be shutdown.
        tellParent(ActorExitRequest())
        # The following two have a 2 second delay each
        assert askParent('name?') is None
        assert askKid('name?') is None
        assert asys.ask(stableKid, 'name?', 0.4) is None


    def test05_RestartedSubActorWithoutRestarts(self, asys):
        unstable_test(asys, 'multiprocUDPBase')
        parent = asys.createActor(NoRestartParent)

        askParent = lambda m: asys.ask(parent, m, 0.5)
        askKid    = lambda m: askParent(TellDaughter(m))

        assert askParent('name?') == parent

        kid = askParent('have a daughter?')
        assert kid is not None
        assert askKid('name?') is None  # dead-lettered, so no response


    def test06_ActorStackShutdown(self, asys):
        unstable_test(asys, 'multiprocUDPBase', 'multiprocQueueBase')
        parent = asys.createActor(RestartParent)

        tellParent = lambda m: asys.tell(parent, m)

        askParent        = lambda m: asys.ask(parent, m, 0.5)
        askKid           = lambda m: askParent(TellDaughter(m))
        askGrandKid      = lambda m: askKid(TellDaughter(m))
        askGreatGrandKid = lambda m: askGrandKid(TellDaughter(m))

        kid = askParent('have a daughter?')
        grandkid = askKid('have a daughter?') # KWQ: multiprocUDP fails here (sometimes) because the first kid fails and has to be re-created, but if the parent sends this grandkid message before the parent gets the child exited then the message just gets dropped because UDP doesn't have any confirmation.  Need to add confirmation to UDP (and can add large message handling at the same time?  Or is there a standard failure for "message too large" and that limit is different for different transports?)
        greatgrandkid = askGrandKid('have a daughter?')
        # n.b. kid, grandkid, and greatgrandkid are not likely
        # useable, because the initial instance of each was a
        # NonStarter and was then restarted and probably received a
        # new ActorAddress.

        assert askParent('name?') is not None
        assert askKid('name?') is not None
        assert askGrandKid('name?') is not None
        assert askGreatGrandKid('name?') is not None

        tellParent(ActorExitRequest())
        #time.sleep(0.2)  # allow actor shutdown requests to propagate

        # False positive (success) if actor system is hung?  Need to
        # check deadletter delivery of these name queries to be sure
        # ActorSystem is fully functional.
        assert askParent('name?') is None
        assert askKid('name?') is None
        assert askGrandKid('name?') is None
        assert askGreatGrandKid('name?') is None


    def test07_DeepActorShutdown(self, asys):
        unstable_test(asys, 'multiprocUDPBase', 'multiprocQueueBase')
        parent = asys.createActor(RestartParent)

        tellParent        = lambda m: asys.tell(parent, m)
        tellKid           = lambda m: tellParent(TellDaughter(m))
        tellGrandKid      = lambda m: tellKid(TellDaughter(m))
        tellGreatGrandKid = lambda m: tellGrandKid(TellDaughter(m))

        askParent        = lambda m: asys.ask(parent, m, 0.5)
        askKid           = lambda m: askParent(TellDaughter(m))
        askGrandKid      = lambda m: askKid(TellDaughter(m))
        askGreatGrandKid = lambda m: askGrandKid(TellDaughter(m))

        kid = askParent('have a daughter?')
        grandkid = askKid('have a daughter?')
        greatgrandkid = askGrandKid('have a daughter?')
        # n.b. kid, grandkid, and greatgrandkid are not useable, see test06 above.

        if asys.base_name == 'multiprocUDPBase':
            time.sleep(0.4)  # see test06 note above; doesn't always work
        assert askParent('name?') is not None
        assert askKid('name?') is not None
        assert askGrandKid('name?') is not None
        assert askGreatGrandKid('name?') is not None

        tellGreatGrandKid(ActorExitRequest())
        #time.sleep(0.2)  # allow actor shutdown requests to propagate

        assert askParent('name?') is not None
        assert askKid('name?') is not None
        assert askGrandKid('name?') is not None
        assert askGreatGrandKid('name?') is not None

        tellGrandKid(ActorExitRequest())
        #time.sleep(0.2)  # allow actor shutdown requests to propagate

        assert askParent('name?') is not None
        assert askKid('name?') is not None
        assert askGrandKid('name?') is not None
        assert askGreatGrandKid('name?') is not None

        # parent is Top Level Actor, so no restarts
        tellParent(ActorExitRequest())
        #time.sleep(0.2)  # allow actor shutdown requests to propagate

        # False positive (success) if actor system is hung?  Need to
        # check deadletter delivery of these name queries to be sure
        # ActorSystem is fully functional.
        assert askParent('name?') is None
        assert askKid('name?') is None
        assert askGrandKid('name?') is None
        assert askGreatGrandKid('name?') is None


    def test08_DeepActorInvoluntaryTermination(self, asys):
        unstable_test(asys, 'multiprocUDPBase', 'multiprocQueueBase')
        parent = asys.createActor(RestartParent)

        tellParent        = lambda m: asys.tell(parent, m)
        tellKid           = lambda m: tellParent(TellDaughter(m))
        tellGrandKid      = lambda m: tellKid(TellDaughter(m))
        tellGreatGrandKid = lambda m: tellGrandKid(TellDaughter(m))

        askParent        = lambda m: asys.ask(parent, m, 0.5)
        askKid           = lambda m: askParent(TellDaughter(m))
        askGrandKid      = lambda m: askKid(TellDaughter(m))
        askGreatGrandKid = lambda m: askGrandKid(TellDaughter(m))

        kid = askParent('have a daughter?')
        grandkid = askKid('have a daughter?')
        greatgrandkid = askGrandKid('have a daughter?')
        # n.b. kid, grandkid, and greatgrandkid are not useable, see test06 above.

        if asys.base_name == 'multiprocUDPBase':
            time.sleep(0.4)  # see test06 note above; doesn't always work
        assert askParent('name?') is not None
        assert askKid('name?') is not None
        assert askGrandKid('name?') is not None
        if asys.base_name == 'multiprocUDPBase':
            time.sleep(0.4)  # see test06 note above; doesn't always work
        assert askGreatGrandKid('name?') is not None

        # Wait a little because first kid dies and second kid has to
        # be created at each level.
        time.sleep(0.2)

        assert not (askParent('poisoned child?'))
        assert not (askKid('poisoned child?'))
        assert not (askGrandKid('poisoned child?'))
        assert not (askGreatGrandKid('poisoned child?'))

        tellParent(Deadly(3))  # kills greatgrandkid
        #time.sleep(0.75)

        assert askParent('name?') is not None
        assert askKid('name?') is not None
        assert askGrandKid('name?') is not None
        assert askGreatGrandKid('name?') is not None

        assert not (askParent('poisoned child?'))
        assert not (askKid('poisoned child?'))
        assert askGrandKid('poisoned child?')
        assert not (askGreatGrandKid('poisoned child?'))

        tellParent(Deadly(1))  # kills kid
        # Kid can restart, but loses knowledge of grandkid or greatgrandkid...
        # looking at test09 below, this is as intended??
        #time.sleep(0.28)  # wait for Deadly message effects to propagate

        assert askParent('name?') is not None
        assert askKid('name?') is not None
        assert askGrandKid('name?') is not None
        assert askGreatGrandKid('name?') is not None

        assert askParent('poisoned child?')
        assert not (askKid('poisoned child?'))
        assert askGrandKid('poisoned child?')
        assert not (askGreatGrandKid('poisoned child?'))

        tellParent(Deadly(0))  # kills parent
        #time.sleep(0.38)  # wait for Deadly message effects to propagate

        # First response from parent should be the
        # PoisonMessage(Deadly), the next should be the response to
        # the 'name?' query.
        r = askParent('name?')
        print('init r is: %s'%str(r))
        while r:
            if isinstance(r, PoisonMessage):
                assert isinstance(r.poisonMessage, Deadly)
                r = askParent('')
                print('next r is: %s'%str(r))
            else:
                assert isinstance(r, ActorAddress)
                break
        assert r is not None

        assert askKid('name?') is not None
        assert askGrandKid('name?') is not None
        assert askGreatGrandKid('name?') is not None

        tellParent(ActorExitRequest())
        assert askParent('name?') is None
        assert askKid('name?') is None
        assert askGrandKid('name?') is None
        assert askGreatGrandKid('name?') is None


    def test09_DeepActorSuicideIsPermanent(self, asys):
        unstable_test(asys, 'multiprocUDPBase', 'multiprocQueueBase')
        parent = asys.createActor(RestartParent)

        tellParent        = lambda m: asys.tell(parent, m)
        tellKid           = lambda m: tellParent(TellDaughter(m))
        tellGrandKid      = lambda m: tellKid(TellDaughter(m))
        tellGreatGrandKid = lambda m: tellGrandKid(TellDaughter(m))

        askParent        = lambda m: asys.ask(parent, m, 0.5)
        askKid           = lambda m: askParent(TellDaughter(m))
        askGrandKid      = lambda m: askKid(TellDaughter(m))
        askGreatGrandKid = lambda m: askGrandKid(TellDaughter(m))

        kid = askParent('have a daughter?')
        grandkid = askKid('have a daughter?')
        greatgrandkid = askGrandKid('have a daughter?')
        # n.b. kid, grandkid, and greatgrandkid are not useable, see test06 above.

        assert askParent('name?') is not None
        assert askKid('name?') is not None
        assert askGrandKid('name?') is not None
        assert askGreatGrandKid('name?') is not None

        assert not (askParent('poisoned child?'))
        assert not (askKid('poisoned child?'))
        assert not (askGrandKid('poisoned child?'))
        assert not (askGreatGrandKid('poisoned child?'))

        kid = askKid('name?')
        grandkid = askGrandKid('name?')
        greatgrandkid = askGreatGrandKid('name?')

        tellParent(KillReq(3))  # kills greatgrandkid

        # Give time for the kill to propagate and the grandkid to
        # replace the greatgrandkid
        time.sleep(0.5)

        assert askParent('name?') is not None
        assert askKid('name?') is not None
        assert askGrandKid('name?') is not None
        assert askGreatGrandKid('name?') is not None

        assert parent == askParent('name?')
        assert kid == askKid('name?')
        assert grandkid == askGrandKid('name?')
        assert greatgrandkid != askGreatGrandKid('name?')
        greatgrandkid = askGreatGrandKid('name?')

        assert not (askParent('poisoned child?'))
        assert not (askKid('poisoned child?'))
        assert not (askGrandKid('poisoned child?'))
        assert not (askGreatGrandKid('poisoned child?'))

        tellParent(KillReq(1))
        # kills kid.  parent will restart kid, but new kid will not
        # know about previous grandkid and greatgrandkid (who should
        # be killed when the kid exits).

        # Give time for the kill to propagate and the parent to
        # replace the kid
        time.sleep(0.5)

        assert askParent('name?') is not None
        assert askKid('name?') is not None
        # Not only does the kid no longer know about grandkids, but
        # asking it to talk to a grandkid will cause an
        # InvalidActorAddress exception, which tells the parent it
        # poisoned the kid.
        assert askGrandKid('name?') is None
        assert askGreatGrandKid('name?') is None

        assert parent == askParent('name?')
        assert kid != askKid('name?')
        #assert asys.ask(grandkid, 'name?', 0.4) is None
        #assert asys.ask(greatgrandkid, 'name?', 0.4) is None

        assert askParent('poisoned child?')
        assert not (askKid('poisoned child?'))

        tellParent(KillReq(0))  # kills parent; no restarts for top level

        askParent('name?')  # throw-away to allow KillReq to be processed.

        assert askParent('name?') is None
        assert askKid('name?') is None
        assert askGrandKid('name?') is None
        assert askGreatGrandKid('name?') is None


    def test_confused_exit(self, asys):
        unstable_test(asys, 'multiprocUDPBase')
        # Verify that even if an actor generates an exception on an
        # ActorExitRequest that it will notify the parent that it
        # exited permanently.
        asys.systemUpdate('dupLogToFile', '/tmp/confused.log')
        confused = asys.createActor(Confused)
        assert "dunno" == asys.ask(confused, 'name?', 0.31)
        confused2 = asys.ask(confused, 'subactor?', 0.31)
        assert "dunno" == asys.ask(confused2, 'name?', 0.31)
        asys.tell(confused2, ActorExitRequest())
        import time
        time.sleep(0.10)  # Allow time for ActorExitRequest to be processed
        assert "permanent" == asys.ask(confused, 'name?', 0.31)
        asys.tell(confused, ActorExitRequest())
        assert asys.ask(confused, 'name?', 0.1) is None

    def test_confused_msgfail(self, asys):
        unstable_test(asys, 'multiprocUDPBase')
        # Verify that if an actor generates an exception on handling
        # an ordinary message that it will notify the parent that the
        # message was Poison but it can continue running.
        asys.systemUpdate('dupLogToFile', '/tmp/confused.log')
        confused = asys.createActor(Confused)
        assert "dunno" == asys.ask(confused, 'name?', 0.31)
        confused2 = asys.ask(confused, 'subactor?', 0.31)
        assert "dunno" == asys.ask(confused2, 'name?', 0.31)
        asys.tell(confused2, Deadly(1))
        import time
        time.sleep(0.10)  # Allow time for ActorExitRequest to be processed

        r = asys.listen(0.20)
        assert isinstance(r, PoisonMessage)
        assert isinstance(r.poisonMessage, Deadly)

        assert "dunno" == asys.ask(confused, 'name?', 0.31)
        assert "dunno" == asys.ask(confused2, 'name?', 0.31)
        asys.tell(confused, ActorExitRequest())
        assert asys.ask(confused, 'name?', 0.1) is None
