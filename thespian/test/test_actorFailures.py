from thespian.actors import *
from thespian.test import *
import time
from datetime import timedelta
import sys


max_replacement_delay = timedelta(seconds=0.35)
max_response_delay = timedelta(seconds=1.0)
max_no_response_delay = timedelta(seconds=0.50)
got_bored_and_left = timedelta(seconds=30)


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
        self._exitWait = False

    def receiveMessage(self, msg, sender):
        if not self._exitWait:
            self.wakeupAfter(got_bored_and_left)
            self._exitWait = True

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
                      msg.msg if isinstance(msg.msg, ActorExitRequest) else
                      PassedMessage(msg.msg, sender))

        elif msg == 'name?':
            self.send(sender, self.myAddress)

        elif isinstance(msg, (BadFish, Fatal)):
            if msg.countdown:
                msg.countdown -= 1
                self.send(self.son or self.daughter, msg)
            else:
                if isinstance(msg, Fatal):
                    sys.exit(0)
                else:
                    raise ValueError('BadFish value received!')
        elif isinstance(msg, PoisonMessage):
            self.poisonedChild = True
            if hasattr(msg.poisonMessage, 'origSender'):
                self.send(msg.poisonMessage.origSender, 'poisoned!')
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
        self._replaced = False
        self._numCreates = 0
        self._numCreatesMax = 5

    def receiveMessage(self, msg, sender):
        if not self._exitWait:
            self.wakeupAfter(got_bored_and_left)
            self._exitWait = True

        if isinstance(msg, ChildActorExited):
            if self._numCreates < self._numCreatesMax:
                self._numCreates += 1
                self._replaced = True
                if msg.childAddress == self.son:
                    self.son = self.createActor(NonStarter)
                elif self.daughter == msg.childAddress:
                    self.daughter = self.createActor(RestartParent)
                self.notifyRestartWaiter()

        elif isinstance(msg, str) and msg == 'wait for replacement':
            self.waitForRestart(msg, sender)

        elif isinstance(msg, PassedMessage) and \
             isinstance(msg.msg, str) and msg.msg == 'wait for replacement':
            self.waitForRestart(msg.msg, msg.origSender)

        elif isinstance(msg, WakeupMessage):
            if msg.delayPeriod == got_bored_and_left:
                # Sometimes the parent gets killed with a harsh signal
                # and cannot shutdown the children, so this cleans up
                # the actor for those tests.
                self.send(self.myAddress, ActorExitRequest())
            else:
                self.notifyRestartWaiter()

        else:
            if isinstance(msg, TellChild):
                self._replaced = False
            super(RestartParent, self).receiveMessage(msg, sender)

    def waitForRestart(self, msg, sender):
        if self._replaced:
            self._replaced = False
            self.send(sender, 'replaced')
        else:
            self.waiter = sender
            self.wakeupAfter(max_replacement_delay)

    def notifyRestartWaiter(self):
        waiter = getattr(self, 'waiter', None)
        if waiter:
            self.send(waiter, 'replaced' if self._replaced else 'not replaced')
            delattr(self, 'waiter')
            self._replaced = False


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
        if isinstance(msg, (ActorExitRequest, BadFish)):
            raise NameError("Who am I?")
        elif msg == "name?":
            self.send(sender, self.name)
        elif msg == "subactor?":
            self.send(sender, self.createActor(Confused))
        elif isinstance(msg, ChildActorExited):
            self.name = 'permanent'


class BadFish(object):
    def __init__(self, v):
        self.countdown = v

class Fatal(BadFish): pass

class KillReq(object):
    def __init__(self, v):
        self.countdown = v


# Note: multiprocQueueBase is marked as unstable because that system
# base assumes that Queue.put() messages have been successfully sent;
# this is not true, and especially when the target has already exited.

class TestFuncActorFailures(object):

    def test01_NonStartingSystemLevelActor(self, asys):
        try:
            nonstarter = asys.createActor(NonStarter)
        except Exception:
            assert True  # this is an acceptable effect
        else:
            # Primary actors (those owned by the ActorSystem itself)
            # are not restarted on failure, so the actor won't
            # actually be recreated.  The "anything" message will
            # actually be routed to the Dead Letter handler (see
            # testDeadLettering).
            assert asys.ask(nonstarter, "anything", 0.3) is None

    def test02_NonStartingSubActorWithRestarts(self, asys):
        parent = asys.createActor(RestartParent)

        tellParent = lambda m: asys.tell(parent, m)

        askParent = lambda m,d=max_response_delay: asys.ask(parent, m, d)
        askKid    = lambda m: askParent(TellDaughter(m))

        r = askParent('name?')
        assert r == parent
        son = askParent('have a son?')
        assert son is not None
        delay_for_next_of_kin_notification(asys)
        r = askParent('wait for replacement')
        assert r == 'replaced'
        r = askParent(TellSon('name?'), max_no_response_delay*5)
        assert r is None

        r = askParent('name?')
        assert r == parent
        tellParent(ActorExitRequest())
        r = askParent('name?', max_no_response_delay)
        assert r is None


    def test03_NonStartingSubActorWithoutRestarts(self, asys):
        parent = asys.createActor(NoRestartParent)

        tellParent = lambda m: asys.tell(parent, m)

        askParent = lambda m: asys.ask(parent, m, max_response_delay)

        assert askParent('name?') == parent
        son = askParent('have a son?')
        assert son is not None  # got an Address back, but Son failed to start
        delay_for_next_of_kin_notification(asys)
        assert askParent(TellSon('name?')) is None  # dead-lettered, so no response

        assert askParent('name?') == parent
        tellParent(ActorExitRequest())
        assert askParent('name?') is None


    def test04_RestartedSubActorWithRestarts(self, asys):
        parent = asys.createActor(RestartParent)
        tellParent = lambda m: asys.tell(parent, m)

        askParent = lambda m: asys.ask(parent, m, max_response_delay)
        askKid    = lambda m: askParent(TellDaughter(m))

        assert askParent('name?') == parent

        kid = askParent('have a daughter?')
        assert kid is not None
        delay_for_next_of_kin_notification(asys)
        r = askParent('wait for replacement')
        assert r == 'replaced'
        assert askKid('name?') is not None

        assert askParent('name?') == parent
        assert askKid('name?') is not None

        stableKid = askKid('name?')
        assert asys.ask(stableKid, 'name?', max_response_delay) == stableKid

        # root Actors are not restarted which should cause children to be shutdown.
        tellParent(ActorExitRequest())
        delay_for_next_of_kin_notification(asys)
        assert askParent('name?') is None
        assert asys.ask(stableKid, 'name?', max_no_response_delay) is None


    def test05_RestartedSubActorWithoutRestarts(self, asys):
        parent = asys.createActor(NoRestartParent)

        askParent = lambda m: asys.ask(parent, m, max_response_delay)
        askKid    = lambda m: askParent(TellDaughter(m))

        assert askParent('name?') == parent

        kid = askParent('have a daughter?')
        assert kid is not None
        delay_for_next_of_kin_notification(asys)
        askParent('name?')  # allow first failure and replacement to occur
        assert askKid('name?') is None  # dead-lettered, so no response


    def test06_ActorStackShutdown(self, asys):
        parent = asys.createActor(RestartParent)

        tellParent = lambda m: asys.tell(parent, m)

        askParent        = lambda m: asys.ask(parent, m, max_response_delay)
        askKid           = lambda m: askParent(TellDaughter(m))
        askGrandKid      = lambda m: askKid(TellDaughter(m))
        askGreatGrandKid = lambda m: askGrandKid(TellDaughter(m))

        kid = askParent('have a daughter?')
        print('kid',kid)
        assert kid
        delay_for_next_of_kin_notification(asys)
        r = askParent('wait for replacement')
        assert r == 'replaced'
        print('getting grandkid')
        grandkid = askKid('have a daughter?')
        print('grandkid',grandkid)
        r = askKid('wait for replacement')
        assert r == 'replaced'
        print('getting greatgrandkid')
        greatgrandkid = askGrandKid('have a daughter?')
        print('greatgrandkid',grandkid)
        r = askGrandKid('wait for replacement')
        assert r == 'replaced'
        # n.b. kid, grandkid, and greatgrandkid are not likely
        # useable, because the initial instance of each was a
        # NonStarter and was then restarted and probably received a
        # new ActorAddress.

        assert askParent('name?') is not None
        assert askKid('name?') is not None
        assert askGrandKid('name?') is not None
        assert askGreatGrandKid('name?') is not None

        tellParent(ActorExitRequest())
        delay_for_next_of_kin_notification(asys)

        # False positive (success) if actor system is hung?  Need to
        # check deadletter delivery of these name queries to be sure
        # ActorSystem is fully functional.
        assert askParent('name?') is None


    def test07_DeepActorShutdown(self, asys):
        parent = asys.createActor(RestartParent)

        tellParent        = lambda m: asys.tell(parent, m)
        tellKid           = lambda m: tellParent(TellDaughter(m))
        tellGrandKid      = lambda m: tellKid(TellDaughter(m))
        tellGreatGrandKid = lambda m: tellGrandKid(TellDaughter(m))

        askParent        = lambda m: asys.ask(parent, m, max_response_delay)
        askKid           = lambda m: askParent(TellDaughter(m))
        askGrandKid      = lambda m: askKid(TellDaughter(m))
        askGreatGrandKid = lambda m: askGrandKid(TellDaughter(m))

        kid = askParent('have a daughter?')
        delay_for_next_of_kin_notification(asys)
        r = askParent('wait for replacement')
        assert r == 'replaced'

        grandkid = askKid('have a daughter?')
        r = askKid('wait for replacement')
        assert r == 'replaced'

        greatgrandkid = askGrandKid('have a daughter?')
        delay_for_next_of_kin_notification(asys)
        r = askGrandKid('wait for replacement')
        assert r == 'replaced'

        # n.b. kid, grandkid, and greatgrandkid are not useable, see test06 above.

        assert askParent('name?') is not None
        assert askKid('name?') is not None
        assert askGrandKid('name?') is not None
        assert askGreatGrandKid('name?') is not None

        tellGreatGrandKid(ActorExitRequest())
        delay_for_next_of_kin_notification(asys)
        r = askGrandKid('wait for replacement')
        assert r == 'replaced'

        assert askParent('name?') is not None
        assert askKid('name?') is not None
        assert askGrandKid('name?') is not None
        assert askGreatGrandKid('name?') is not None

        tellGrandKid(ActorExitRequest())
        delay_for_next_of_kin_notification(asys)
        r = askKid('wait for replacement')
        assert r == 'replaced'

        assert askParent('name?') is not None
        assert askKid('name?') is not None
        assert askGrandKid('name?') is not None
        assert askGreatGrandKid('name?') is not None

        # parent is Top Level Actor, so no restarts
        tellParent(ActorExitRequest())
        delay_for_next_of_kin_notification(asys)

        # False positive (success) if actor system is hung?  Need to
        # check deadletter delivery of these name queries to be sure
        # ActorSystem is fully functional.
        assert askParent('name?') is None


    def test08_DeepActorInvoluntaryTermination(self, asys):
        actor_system_unsupported(asys, "simpleSystemBase") # Fatal message causes sys.exit(0)
        parent = asys.createActor(RestartParent)

        tellParent        = lambda m: asys.tell(parent, m)
        tellKid           = lambda m: tellParent(TellDaughter(m))
        tellGrandKid      = lambda m: tellKid(TellDaughter(m))
        tellGreatGrandKid = lambda m: tellGrandKid(TellDaughter(m))

        askParent        = lambda m: asys.ask(parent, m, max_response_delay)
        askKid           = lambda m: askParent(TellDaughter(m))
        askGrandKid      = lambda m: askKid(TellDaughter(m))
        askGreatGrandKid = lambda m: askGrandKid(TellDaughter(m))

        kid = askParent('have a daughter?')
        r = askParent('wait for replacement')
        assert r == 'replaced'
        grandkid = askKid('have a daughter?')
        r = askKid('wait for replacement')
        assert r == 'replaced'
        greatgrandkid = askGrandKid('have a daughter?')
        delay_for_next_of_kin_notification(asys)
        r = askGrandKid('wait for replacement')
        assert r == 'replaced'

        # n.b. kid, grandkid, and greatgrandkid are not useable, see test06 above.

        if asys.base_name == 'multiprocUDPBase' or 'TXRouting' in asys.base_name:
            time.sleep(0.4)  # see test06 note above; doesn't always work
        name_parent = askParent('name?')
        assert name_parent is not None
        name_kid = askKid('name?')
        assert name_kid is not None
        name_grandkid = askGrandKid('name?')
        assert name_grandkid is not None
        if asys.base_name == 'multiprocUDPBase':
            time.sleep(0.4)  # see test06 note above; doesn't always work
        name_great_grandkid = askGreatGrandKid('name?')
        assert name_great_grandkid is not None

        r = askParent('poisoned child?')
        assert not r
        r = askKid('poisoned child?')
        assert not r
        r = askGrandKid('poisoned child?')
        assert not r
        r = askGreatGrandKid('poisoned child?')
        assert not r

        tellParent(BadFish(3))  # does not kill, but causes poisonmessage rejection
        delay_for_next_of_kin_notification(asys)
        r = askGrandKid('wait for replacement')
        assert r == 'not replaced'

        r = askParent('name?')
        assert r == name_parent
        r = askKid('name?')
        assert r == name_kid
        r = askGrandKid('name?')
        assert r == name_grandkid
        r = askGreatGrandKid('name?')
        assert r == name_great_grandkid

        r = askParent('poisoned child?')
        assert not r
        r =  askKid('poisoned child?')
        assert not r
        r = askGrandKid('poisoned child?')
        assert r
        r = askGreatGrandKid('poisoned child?')
        assert not r

        tellParent(Fatal(3))  # kills greatgrandkid
        delay_for_next_of_kin_notification(asys)
        r = askGrandKid('wait for replacement')
        assert r == 'replaced'

        r = askParent('name?')
        assert r is not None
        assert r == name_parent
        r = askKid('name?')
        assert r is not None
        assert r == name_kid
        r = askGrandKid('name?')
        assert r is not None
        assert r == name_grandkid
        r = askGreatGrandKid('name?')
        assert r is not None
        assert r != name_great_grandkid

        tellParent(Fatal(1))  # kills kid
        # Kid can restart, but loses knowledge of grandkid or greatgrandkid...
        # looking at test09 below, this is as intended??
        delay_for_next_of_kin_notification(asys)
        r = askParent('wait for replacement')
        assert r == 'replaced'

        r = askParent('name?')
        assert r is not None
        r = askKid('name?')
        assert r is not None
        r = askGrandKid('name?')
        assert r is not None
        r = askGreatGrandKid('name?')
        assert r is not None

        tellParent(BadFish(0))  # poisons parent
        delay_for_next_of_kin_notification(asys)

        # First response from parent should be the
        # PoisonMessage(BadFish), the next should be the response to
        # the 'name?' query.
        r = askParent('name?')
        print('init r is: %s'%str(r))
        while r:
            if isinstance(r, PoisonMessage):
                assert isinstance(r.poisonMessage, BadFish)
                r = askParent('')
                print('next r is: %s'%str(r))
            else:
                assert isinstance(r, ActorAddress)
                break
        assert r is not None

        r = askKid('name?')
        assert r is not None
        r = askGrandKid('name?')
        assert r is not None
        r = askGreatGrandKid('name?')
        assert r is not None

        tellParent(ActorExitRequest())
        r = askParent('name?')
        assert r is None


    def test09_DeepActorSuicideIsPermanent(self, asys):
        parent = asys.createActor(RestartParent)

        tellParent        = lambda m: asys.tell(parent, m)
        tellKid           = lambda m: tellParent(TellDaughter(m))
        tellGrandKid      = lambda m: tellKid(TellDaughter(m))
        tellGreatGrandKid = lambda m: tellGrandKid(TellDaughter(m))

        askParent        = lambda m: asys.ask(parent, m, max_response_delay)
        askKid           = lambda m: askParent(TellDaughter(m))
        askGrandKid      = lambda m: askKid(TellDaughter(m))
        askGreatGrandKid = lambda m: askGrandKid(TellDaughter(m))

        kid = askParent('have a daughter?')
        assert kid
        r = askParent('wait for replacement')
        assert r == 'replaced'

        grandkid = askKid('have a daughter?')
        assert grandkid
        r = askKid('wait for replacement')
        assert r == 'replaced'

        greatgrandkid = askGrandKid('have a daughter?')
        assert greatgrandkid
        r = askGrandKid('wait for replacement')
        assert r == 'replaced'

        # n.b. kid, grandkid, and greatgrandkid are not useable, see test06 above.

        r = askParent('name?')
        assert r is not None

        r = askKid('name?')
        assert r is not None
        r = askGrandKid('name?')
        assert r is not None
        r = askGreatGrandKid('name?')
        assert r is not None

        r = askParent('poisoned child?')
        assert not r
        r = askKid('poisoned child?')
        assert not r
        r = askGrandKid('poisoned child?')
        assert not r
        r = askGreatGrandKid('poisoned child?')
        assert not r

        kid = askKid('name?')
        grandkid = askGrandKid('name?')
        greatgrandkid = askGreatGrandKid('name?')

        tellParent(KillReq(3))  # kills greatgrandkid
        delay_for_next_of_kin_notification(asys)

        # Give time for the kill to propagate and the grandkid to
        # replace the greatgrandkid
        r = askGrandKid('wait for replacement')
        assert r == 'replaced'

        r = askParent('name?')
        assert r is not None
        r = askKid('name?')
        assert r is not None
        r = askGrandKid('name?')
        assert r is not None
        r = askGreatGrandKid('name?')
        assert r is not None

        r = askParent('name?')
        assert parent == r
        r = askKid('name?')
        assert kid == r
        r = askGrandKid('name?')
        assert grandkid == r
        r = askGreatGrandKid('name?')
        assert greatgrandkid != r
        greatgrandkid = askGreatGrandKid('name?')

        r = askParent('poisoned child?')
        assert not r
        r = askKid('poisoned child?')
        assert not r
        r = askGrandKid('poisoned child?')
        assert not r
        r = askGreatGrandKid('poisoned child?')
        assert not r

        tellParent(KillReq(1))
        # kills kid.  parent will restart kid, but new kid will not
        # know about previous grandkid and greatgrandkid (who should
        # be killed when the kid exits).

        # Give time for the kill to propagate and the parent to
        # replace the kid
        delay_for_next_of_kin_notification(asys)
        r = askParent('wait for replacement')
        assert r == 'replaced'

        r = askParent('name?')
        assert r is not None
        r = askKid('name?')
        assert r is not None
        # Not only does the kid no longer know about grandkids, but
        # asking it to talk to a grandkid will cause an
        # InvalidActorAddress exception, which tells the parent it
        # poisoned the kid.
        r = askGrandKid('name?')
        assert r == 'poisoned!'
        r = askGreatGrandKid('name?')
        assert r == 'poisoned!'

        r = askParent('name?')
        assert parent == r
        r = askKid('name?')
        assert kid != r
        #assert asys.ask(grandkid, 'name?', 0.4) is None
        #assert asys.ask(greatgrandkid, 'name?', 0.4) is None

        r = askParent('poisoned child?')
        assert r
        r = askKid('poisoned child?')
        assert not r

        tellParent(KillReq(0))  # kills parent; no restarts for top level
        delay_for_next_of_kin_notification(asys)

        askParent('name?')  # throw-away to allow KillReq to be processed.

        r = askParent('name?')
        assert r is None


    def test_confused_exit(self, asys):
        # Verify that even if an actor generates an exception on an
        # ActorExitRequest that it will notify the parent that it
        # exited permanently.
        asys.systemUpdate('dupLogToFile', '/tmp/confused.log')
        confused = asys.createActor(Confused)
        assert "dunno" == asys.ask(confused, 'name?', max_response_delay)
        confused2 = asys.ask(confused, 'subactor?', max_response_delay)
        assert "dunno" == asys.ask(confused2, 'name?', max_response_delay)
        asys.tell(confused2, ActorExitRequest())
        asys.ask(confused, 'subactor?', max_response_delay)  # Allow time for ActorExitRequest to be processed
        assert "permanent" == asys.ask(confused, 'name?', max_response_delay)
        asys.tell(confused, ActorExitRequest())
        assert asys.ask(confused, 'name?', max_response_delay) is None

    def test_confused_msgfail(self, asys):
        # Verify that if an actor generates an exception on handling
        # an ordinary message that it will notify the parent that the
        # message was Poison but it can continue running.
        asys.systemUpdate('dupLogToFile', '/tmp/confused.log')
        confused = asys.createActor(Confused)
        assert "dunno" == asys.ask(confused, 'name?', max_response_delay)
        confused2 = asys.ask(confused, 'subactor?', max_response_delay)
        assert "dunno" == asys.ask(confused2, 'name?', max_response_delay)
        asys.tell(confused2, BadFish(1))
        # Need to use actual time.sleep so that the poison message
        # response is still available.
        delay_for_next_of_kin_notification(asys)
        #asys.ask(confused, 'subactor?', max_response_delay)

        r = asys.listen(max_response_delay)
        assert isinstance(r, PoisonMessage)
        assert isinstance(r.poisonMessage, BadFish)

        assert "dunno" == asys.ask(confused, 'name?', max_response_delay)
        assert "dunno" == asys.ask(confused2, 'name?', max_response_delay)
        asys.tell(confused, ActorExitRequest())
        assert asys.ask(confused, 'name?', max_response_delay) is None

    def test_abrupt_child_exit(self, asys):
        actor_system_unsupported(asys, 'simpleSystemBase')
        parent = asys.createActor(RestartParent)

        kid = asys.ask(parent, 'have a daughter?', max_response_delay)

        # n.b. first kid will die and be replaced by the parent, so
        # the kid address here is dead and parent has a new one.

        delay_for_next_of_kin_notification(asys)
        kid = asys.ask(parent, 'have a daughter?', max_response_delay)


        r1 = asys.ask(parent, TellDaughter('name?'), max_response_delay)
        assert r1 is not None

        asys.tell(kid, Fatal(0))
        delay_for_next_of_kin_notification(asys)

        # Parent should have been notified of child's exit and started
        # another, so the following should succeed, but return a
        # different value than previously.
        r2 = asys.ask(parent, TellDaughter('name?'), max_response_delay)
        assert r2 is not None
        assert r1 != r2
