import unittest
import thespian.test.helpers
from thespian.actors import *
from thespian.test import ActorSystemTestCase
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


class TestASimpleSystem(ActorSystemTestCase):
    testbase='Simple'
    scope='func'

    def test01_NonStartingSystemLevelActor(self):
        nonstarter = ActorSystem().createActor(NonStarter)
        # just finish, make sure no exception is thrown.  Primary
        # actors (those owned by the ActorSystem itself) are not
        # restarted on failure, so the actor won't actually be
        # recreated.  The "anything" message will actually be routed
        # to the Dead Letter handler (see testDeadLettering).
        self.assertIsNone(ActorSystem().ask(nonstarter, "anything", 0.3))

    def test02_NonStartingSubActorWithRestarts(self):
        parent = ActorSystem().createActor(RestartParent)

        tellParent = lambda m: ActorSystem().tell(parent, m)

        askParent        = lambda m: ActorSystem().ask(parent, m, 2)
        askKid    = lambda m: askParent(TellDaughter(m))

        self.assertEqual(askParent('name?'), parent)
        son = askParent('have a son?')
        self.assertIsNotNone(son)
        self.assertIsNone(askParent(TellSon('name?')))

        self.assertEqual(askParent('name?'), parent)
        tellParent(ActorExitRequest())
        self.assertIsNone(askParent('name?'))


    def test03_NonStartingSubActorWithoutRestarts(self):
        parent = ActorSystem().createActor(NoRestartParent)

        tellParent = lambda m: ActorSystem().tell(parent, m)

        askParent        = lambda m: ActorSystem().ask(parent, m, 0.5)

        self.assertEqual(askParent('name?'), parent)
        son = askParent('have a son?')
        self.assertIsNotNone(son)  # got an Address back, but Son failed to start
        self.assertIsNone(askParent(TellSon('name?')))  # dead-lettered, so no response

        self.assertEqual(askParent('name?'), parent)
        tellParent(ActorExitRequest())
        self.assertIsNone(askParent('name?'))


    def test04_RestartedSubActorWithRestarts(self):
        parent = ActorSystem().createActor(RestartParent)

        tellParent = lambda m: ActorSystem().tell(parent, m)

        askParent = lambda m: ActorSystem().ask(parent, m, 0.5)
        askKid    = lambda m: askParent(TellDaughter(m))

        self.assertEqual(askParent('name?'), parent)

        kid = askParent('have a daughter?')
        self.assertIsNotNone(kid)
        self.assertIsNotNone(askKid('name?'))

        self.assertEqual(askParent('name?'), parent)
        self.assertIsNotNone(askKid('name?'))

        stableKid = askKid('name?')
        self.assertEqual(ActorSystem().ask(stableKid, 'name?', 0.4), stableKid)

        # root Actors are not restarted which should cause children to be shutdown.
        tellParent(ActorExitRequest())
        # The following two have a 2 second delay each
        self.assertIsNone(askParent('name?'))
        self.assertIsNone(askKid('name?'))
        self.assertIsNone(ActorSystem().ask(stableKid, 'name?', 0.4))


    def test05_RestartedSubActorWithoutRestarts(self):
        parent = ActorSystem().createActor(NoRestartParent)

        askParent = lambda m: ActorSystem().ask(parent, m, 0.5)
        askKid    = lambda m: askParent(TellDaughter(m))

        self.assertEqual(askParent('name?'), parent)

        kid = askParent('have a daughter?')
        self.assertIsNotNone(kid)
        self.assertIsNone(askKid('name?'))  # dead-lettered, so no response


    def test06_ActorStackShutdown(self):
        parent = ActorSystem().createActor(RestartParent)

        tellParent = lambda m: ActorSystem().tell(parent, m)

        askParent        = lambda m: ActorSystem().ask(parent, m, 0.5)
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

        self.assertIsNotNone(askParent('name?'))
        self.assertIsNotNone(askKid('name?'))
        self.assertIsNotNone(askGrandKid('name?'))
        self.assertIsNotNone(askGreatGrandKid('name?'))

        tellParent(ActorExitRequest())
        #time.sleep(0.2)  # allow actor shutdown requests to propagate

        # False positive (success) if actor system is hung?  Need to
        # check deadletter delivery of these name queries to be sure
        # ActorSystem is fully functional.
        self.assertIsNone(askParent('name?'))
        self.assertIsNone(askKid('name?'))
        self.assertIsNone(askGrandKid('name?'))
        self.assertIsNone(askGreatGrandKid('name?'))


    def test07_DeepActorShutdown(self):
        parent = ActorSystem().createActor(RestartParent)

        tellParent        = lambda m: ActorSystem().tell(parent, m)
        tellKid           = lambda m: tellParent(TellDaughter(m))
        tellGrandKid      = lambda m: tellKid(TellDaughter(m))
        tellGreatGrandKid = lambda m: tellGrandKid(TellDaughter(m))

        askParent        = lambda m: ActorSystem().ask(parent, m, 0.5)
        askKid           = lambda m: askParent(TellDaughter(m))
        askGrandKid      = lambda m: askKid(TellDaughter(m))
        askGreatGrandKid = lambda m: askGrandKid(TellDaughter(m))

        kid = askParent('have a daughter?')
        grandkid = askKid('have a daughter?')
        greatgrandkid = askGrandKid('have a daughter?')
        # n.b. kid, grandkid, and greatgrandkid are not useable, see test06 above.

        if self.testbase == 'MultiprocUDP':
            time.sleep(0.4)  # see test06 note above; doesn't always work
        self.assertIsNotNone(askParent('name?'))
        self.assertIsNotNone(askKid('name?'))
        self.assertIsNotNone(askGrandKid('name?'))
        self.assertIsNotNone(askGreatGrandKid('name?'))

        tellGreatGrandKid(ActorExitRequest())
        #time.sleep(0.2)  # allow actor shutdown requests to propagate

        self.assertIsNotNone(askParent('name?'))
        self.assertIsNotNone(askKid('name?'))
        self.assertIsNotNone(askGrandKid('name?'))
        self.assertIsNotNone(askGreatGrandKid('name?'))

        tellGrandKid(ActorExitRequest())
        #time.sleep(0.2)  # allow actor shutdown requests to propagate

        self.assertIsNotNone(askParent('name?'))
        self.assertIsNotNone(askKid('name?'))
        self.assertIsNotNone(askGrandKid('name?'))
        self.assertIsNotNone(askGreatGrandKid('name?'))

        # parent is Top Level Actor, so no restarts
        tellParent(ActorExitRequest())
        #time.sleep(0.2)  # allow actor shutdown requests to propagate

        # False positive (success) if actor system is hung?  Need to
        # check deadletter delivery of these name queries to be sure
        # ActorSystem is fully functional.
        self.assertIsNone(askParent('name?'))
        self.assertIsNone(askKid('name?'))
        self.assertIsNone(askGrandKid('name?'))
        self.assertIsNone(askGreatGrandKid('name?'))


    def test08_DeepActorInvoluntaryTermination(self):
        parent = ActorSystem().createActor(RestartParent)

        tellParent        = lambda m: ActorSystem().tell(parent, m)
        tellKid           = lambda m: tellParent(TellDaughter(m))
        tellGrandKid      = lambda m: tellKid(TellDaughter(m))
        tellGreatGrandKid = lambda m: tellGrandKid(TellDaughter(m))

        askParent        = lambda m: ActorSystem().ask(parent, m, 0.5)
        askKid           = lambda m: askParent(TellDaughter(m))
        askGrandKid      = lambda m: askKid(TellDaughter(m))
        askGreatGrandKid = lambda m: askGrandKid(TellDaughter(m))

        kid = askParent('have a daughter?')
        grandkid = askKid('have a daughter?')
        greatgrandkid = askGrandKid('have a daughter?')
        # n.b. kid, grandkid, and greatgrandkid are not useable, see test06 above.

        if self.testbase == 'MultiprocUDP':
            time.sleep(0.4)  # see test06 note above; doesn't always work
        self.assertIsNotNone(askParent('name?'))
        self.assertIsNotNone(askKid('name?'))
        self.assertIsNotNone(askGrandKid('name?'))
        if self.testbase == 'MultiprocUDP':
            time.sleep(0.4)  # see test06 note above; doesn't always work
        self.assertIsNotNone(askGreatGrandKid('name?'))

        # Wait a little because first kid dies and second kid has to
        # be created at each level.
        time.sleep(0.2)

        self.assertFalse(askParent('poisoned child?'))
        self.assertFalse(askKid('poisoned child?'))
        self.assertFalse(askGrandKid('poisoned child?'))
        self.assertFalse(askGreatGrandKid('poisoned child?'))

        tellParent(Deadly(3))  # kills greatgrandkid
        #time.sleep(0.75)

        self.assertIsNotNone(askParent('name?'))
        self.assertIsNotNone(askKid('name?'))
        self.assertIsNotNone(askGrandKid('name?'))
        self.assertIsNotNone(askGreatGrandKid('name?'))

        self.assertFalse(askParent('poisoned child?'))
        self.assertFalse(askKid('poisoned child?'))
        self.assertTrue(askGrandKid('poisoned child?'))
        self.assertFalse(askGreatGrandKid('poisoned child?'))

        tellParent(Deadly(1))  # kills kid
        # Kid can restart, but loses knowledge of grandkid or greatgrandkid...
        # looking at test09 below, this is as intended??
        #time.sleep(0.28)  # wait for Deadly message effects to propagate

        self.assertIsNotNone(askParent('name?'))
        self.assertIsNotNone(askKid('name?'))
        self.assertIsNotNone(askGrandKid('name?'))
        self.assertIsNotNone(askGreatGrandKid('name?'))

        self.assertTrue(askParent('poisoned child?'))
        self.assertFalse(askKid('poisoned child?'))
        self.assertTrue(askGrandKid('poisoned child?'))
        self.assertFalse(askGreatGrandKid('poisoned child?'))

        tellParent(Deadly(0))  # kills parent
        #time.sleep(0.38)  # wait for Deadly message effects to propagate

        # First response from parent should be the
        # PoisonMessage(Deadly), the next should be the response to
        # the 'name?' query.
        r = askParent('name?')
        print('init r is: %s'%str(r))
        while r:
            if isinstance(r, PoisonMessage):
                self.assertIsInstance(r.poisonMessage, Deadly)
                r = askParent('')
                print('next r is: %s'%str(r))
            else:
                self.assertIsInstance(r, ActorAddress)
                break
        self.assertIsNotNone(r)

        self.assertIsNotNone(askKid('name?'))
        self.assertIsNotNone(askGrandKid('name?'))
        self.assertIsNotNone(askGreatGrandKid('name?'))

        tellParent(ActorExitRequest())
        self.assertIsNone(askParent('name?'))
        self.assertIsNone(askKid('name?'))
        self.assertIsNone(askGrandKid('name?'))
        self.assertIsNone(askGreatGrandKid('name?'))


    def test09_DeepActorSuicideIsPermanent(self):
        parent = ActorSystem().createActor(RestartParent)

        tellParent        = lambda m: ActorSystem().tell(parent, m)
        tellKid           = lambda m: tellParent(TellDaughter(m))
        tellGrandKid      = lambda m: tellKid(TellDaughter(m))
        tellGreatGrandKid = lambda m: tellGrandKid(TellDaughter(m))

        askParent        = lambda m: ActorSystem().ask(parent, m, 0.5)
        askKid           = lambda m: askParent(TellDaughter(m))
        askGrandKid      = lambda m: askKid(TellDaughter(m))
        askGreatGrandKid = lambda m: askGrandKid(TellDaughter(m))

        kid = askParent('have a daughter?')
        grandkid = askKid('have a daughter?')
        greatgrandkid = askGrandKid('have a daughter?')
        # n.b. kid, grandkid, and greatgrandkid are not useable, see test06 above.

        self.assertIsNotNone(askParent('name?'))
        self.assertIsNotNone(askKid('name?'))
        self.assertIsNotNone(askGrandKid('name?'))
        self.assertIsNotNone(askGreatGrandKid('name?'))

        self.assertFalse(askParent('poisoned child?'))
        self.assertFalse(askKid('poisoned child?'))
        self.assertFalse(askGrandKid('poisoned child?'))
        self.assertFalse(askGreatGrandKid('poisoned child?'))

        kid = askKid('name?')
        grandkid = askGrandKid('name?')
        greatgrandkid = askGreatGrandKid('name?')

        tellParent(KillReq(3))  # kills greatgrandkid

        # Give time for the kill to propagate and the grandkid to
        # replace the greatgrandkid
        time.sleep(0.5)

        self.assertIsNotNone(askParent('name?'))
        self.assertIsNotNone(askKid('name?'))
        self.assertIsNotNone(askGrandKid('name?'))
        self.assertIsNotNone(askGreatGrandKid('name?'))

        self.assertEqual(parent, askParent('name?'))
        self.assertEqual(kid, askKid('name?'))
        self.assertEqual(grandkid, askGrandKid('name?'))
        self.assertNotEqual(greatgrandkid, askGreatGrandKid('name?'))
        greatgrandkid = askGreatGrandKid('name?')

        self.assertFalse(askParent('poisoned child?'))
        self.assertFalse(askKid('poisoned child?'))
        self.assertFalse(askGrandKid('poisoned child?'))
        self.assertFalse(askGreatGrandKid('poisoned child?'))

        tellParent(KillReq(1))
        # kills kid.  parent will restart kid, but new kid will not
        # know about previous grandkid and greatgrandkid (who should
        # be killed when the kid exits).

        # Give time for the kill to propagate and the parent to
        # replace the kid
        time.sleep(0.5)

        self.assertIsNotNone(askParent('name?'))
        self.assertIsNotNone(askKid('name?'))
        # Not only does the kid no longer know about grandkids, but
        # asking it to talk to a grandkid will cause an
        # InvalidActorAddress exception, which tells the parent it
        # poisoned the kid.
        self.assertIsNone(askGrandKid('name?'))
        self.assertIsNone(askGreatGrandKid('name?'))

        self.assertEqual(parent, askParent('name?'))
        self.assertNotEqual(kid, askKid('name?'))
        #self.assertIsNone(ActorSystem().ask(grandkid, 'name?', 0.4))
        #self.assertIsNone(ActorSystem().ask(greatgrandkid, 'name?', 0.4))

        self.assertTrue(askParent('poisoned child?'))
        self.assertFalse(askKid('poisoned child?'))

        tellParent(KillReq(0))  # kills parent; no restarts for top level

        askParent('name?')  # throw-away to allow KillReq to be processed.

        self.assertIsNone(askParent('name?'))
        self.assertIsNone(askKid('name?'))
        self.assertIsNone(askGrandKid('name?'))
        self.assertIsNone(askGreatGrandKid('name?'))


    def test_confused_exit(self):
        # Verify that even if an actor generates an exception on an
        # ActorExitRequest that it will notify the parent that it
        # exited permanently.
        ActorSystem().systemUpdate('dupLogToFile', '/tmp/confused.log')
        confused = ActorSystem().createActor(Confused)
        self.assertEqual("dunno", ActorSystem().ask(confused, 'name?', 0.31))
        confused2 = ActorSystem().ask(confused, 'subactor?', 0.31)
        self.assertEqual("dunno", ActorSystem().ask(confused2, 'name?', 0.31))
        ActorSystem().tell(confused2, ActorExitRequest())
        import time
        time.sleep(0.10)  # Allow time for ActorExitRequest to be processed
        self.assertEqual("permanent", ActorSystem().ask(confused, 'name?', 0.31))
        ActorSystem().tell(confused, ActorExitRequest())
        self.assertEqual(None, ActorSystem().ask(confused, 'name?', 0.1))

    def test_confused_msgfail(self):
        # Verify that if an actor generates an exception on handling
        # an ordinary message that it will notify the parent that the
        # message was Poison but it can continue running.
        ActorSystem().systemUpdate('dupLogToFile', '/tmp/confused.log')
        confused = ActorSystem().createActor(Confused)
        self.assertEqual("dunno", ActorSystem().ask(confused, 'name?', 0.31))
        confused2 = ActorSystem().ask(confused, 'subactor?', 0.31)
        self.assertEqual("dunno", ActorSystem().ask(confused2, 'name?', 0.31))
        ActorSystem().tell(confused2, Deadly(1))
        import time
        time.sleep(0.10)  # Allow time for ActorExitRequest to be processed

        r = ActorSystem().listen(0.20)
        self.assertIsInstance(r, PoisonMessage)
        self.assertIsInstance(r.poisonMessage, Deadly)

        self.assertEqual("dunno", ActorSystem().ask(confused, 'name?', 0.31))
        self.assertEqual("dunno", ActorSystem().ask(confused2, 'name?', 0.31))
        ActorSystem().tell(confused, ActorExitRequest())
        self.assertIsNone(ActorSystem().ask(confused, 'name?', 0.1))



class TestMultiprocUDPSystem(TestASimpleSystem):
    testbase='MultiprocUDP'
    unstable=True # see note in test06
    def setUp(self):
        self.setSystemBase('multiprocUDPBase')
        super(TestMultiprocUDPSystem, self).setUp()

class TestMultiprocTCPSystem(TestASimpleSystem):
    testbase='MultiprocTCP'
    def setUp(self):
        self.setSystemBase('multiprocTCPBase')
        super(TestMultiprocTCPSystem, self).setUp()

class TestMultiprocQueueSystem(TestASimpleSystem):
    testbase='MultiprocQueue'
    def setUp(self):
        self.setSystemBase('multiprocQueueBase')
        super(TestMultiprocQueueSystem, self).setUp()

