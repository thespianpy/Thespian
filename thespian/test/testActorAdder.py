'''This is a unit test of the Actor system.  It creates a crazy-quilt
of actors which route a request through the network based on the
request type (addEvery, addOdd, addEven, addFives, addSevens, etc.).
The intent is to ensure that various paths using different passed
ActorAddresses work, and that multiple request can be injected into
the system at once and still be handled correctly.'''

import unittest
import thespian.test.helpers
from thespian.actors import *
from thespian.test import TestSystem


class addEvery(object):
    "Message causing the enclosed value to be incremented by one by each Actor."
    def __init__(self, value=0, asker=None, actorList=[]):
        self.value = value
        self.asker = asker
        self.step = 1
        if actorList:
            self.actorList = actorList


class DropResults(Actor):
    def receiveMessage(self, msg, sender):
        pass


class AddOne(Actor):
    "This actor adds one to the input value and then passes it to the next actor."
    def receiveMessage(self, msg, sender):
#        if isinstance(msg, ActorAddress):
#            self.next_adder = msg   # next actor
        if isinstance(msg, addEvery):
            if not msg.asker: msg.asker = sender  # start of chain, remember asker
            msg.value = msg.value + 1
            if msg.actorList:
                next_addr = msg.actorList.pop()
                self.send(next_addr, msg)
            else:
                self.send(msg.asker, msg)


class AddTen(Actor):
    def __init__(self):
        self.next_adder = {}
        self.adders = None
    def receiveMessage(self, msg, sender):
        if not self.adders:
            # n.b. would prefer to do this initialization in __init__, but that's not supported ATM.  - KWQ 2013.10.30
            # Create 10 AddOne Actors
            self.adders = [ self.createActor(AddOne) for x in range(10) ]
        if isinstance(msg, type((1,2))):
            self.next_adder[msg[0]] = msg[1]
        elif isinstance(msg, addEvery):
            if not msg.asker: msg.asker = sender  # start of chain, remember asker
            if hasattr(msg, 'parentmsg'):
                # completion chain is returning here
                parmsg = msg.parentmsg
                parmsg.value = msg.value
                self.send(self.next_adder.get(parmsg.step, parmsg.asker), parmsg)
            else:
                # start of sub-adder chain
                submsg = addEvery(msg.value, actorList = self.adders[1:])
                submsg.parentmsg = msg
                self.send(self.adders[0], submsg)
        elif msg == "Names of Children?":
            self.send(sender, self.adders)


class TestASimpleSystem(unittest.TestCase):
    testbase='Simple'
    scope='func'

    sysbase = 'simpleSystemBase'

    def test01_TenEvery(self):
        with TestSystem(self.sysbase, {'Admin Port': 10200+1}) as asys:
            addOnes = [ asys.createActor(AddOne) for x in range(10) ]
            for (frmA, toA) in zip(addOnes, addOnes[1:]):
                asys.tell(frmA, (1, toA))
            sum = asys.ask(addOnes[0], addEvery(actorList = addOnes[1:]))
            self.assertEqual(sum.value, 10)
            sum2 = asys.ask(addOnes[0], addEvery(actorList = addOnes[1:]))
            self.assertEqual(sum2.value, 10)

    def test02_TenEvens(self):
        with TestSystem(self.sysbase, {'Admin Port': 10200+2}) as asys:
            addOnes = [ asys.createActor(AddOne) for x in range(10) ]
            for (frmA, toA) in zip(addOnes, addOnes[1:]):
                asys.tell(frmA, (1, toA))
            for (frmA, toA) in zip(addOnes[0:], addOnes[2:]):
                asys.tell(frmA, (2, toA))
            sum = asys.ask(addOnes[0], addEvery(actorList = addOnes[1:]))
            self.assertEqual(sum.value, 10)
            sum2 = asys.ask(addOnes[0], addEvery(actorList = [addOnes[I] for I in range(2, len(addOnes),2)]))
            self.assertEqual(sum2.value, 5)
            sum3 = asys.ask(addOnes[1], addEvery(actorList = [addOnes[I] for I in range(3, len(addOnes),2)]))
            self.assertEqual(sum3.value, 5)
            sum4 = asys.ask(addOnes[4], addEvery(actorList = [addOnes[I] for I in range(6, len(addOnes),2)]))
            self.assertEqual(sum4.value, 3)


    def test03_LotsOfActorsEvery(self):
        with TestSystem(self.sysbase, {'Admin Port': 10200+3}) as asys:
            addOnes = [ asys.createActor(AddOne) for x in range(50) ]
            for (frmA, toA) in zip(addOnes, addOnes[1:]):
                asys.tell(frmA, (1, toA))
            sum = asys.ask(addOnes[0], addEvery(actorList = addOnes[1:]))
            self.assertEqual(sum.value, 50)
            sum2 = asys.ask(addOnes[0], addEvery(actorList = addOnes[1:]))
            self.assertEqual(sum2.value, 50)
            for killA in addOnes:
                asys.tell(killA, ActorExitRequest())

    def test04_LotsOfActorsEveryTen(self):
        with TestSystem(self.sysbase, {'Admin Port': 10200+4}) as asys:
            # Create a set of addTens
            addTens = [ asys.createActor(AddTen) for x in range(0, 50, 10) ]
            # Point each addTens to the next one to make a chain.  Each
            # addTens will automatically make 10 addOnes children.
            for (frmA, toA) in zip(addTens, addTens[1:]):
                asys.tell(frmA, (1, toA))

            sum = asys.ask(addTens[0], addEvery())
            self.assertEqual(sum.value, 50)
            sum2 = asys.ask(addTens[0], addEvery())
            self.assertEqual(sum2.value, 50)
            for killA in addTens:
                asys.tell(killA, ActorExitRequest())

    def test05_UniqueAddresses(self):
        with TestSystem(self.sysbase, {'Admin Port': 10200+5}) as sys:
            addTen = sys.createActor(AddTen)
            children = sys.ask(addTen, "Names of Children?")
            uniqueAddresses = [addTen]
            for each in children:
                self.assertNotIn(each, uniqueAddresses)
                uniqueAddresses.append(each)
            self.assertEqual(11, len(uniqueAddresses))
            #Not needed: asys.tell(addTen, ActorExitRequest())


    def test06_TenEveryTen(self):
        with TestSystem(self.sysbase, {'Admin Port': 10200+6}) as asys:
            addTen = asys.createActor(AddTen)
            sum = asys.ask(addTen, addEvery())
            self.assertEqual(sum.value, 10)

    def test07_LotsOfActorsEveryTenWithBackground(self):
        with TestSystem(self.sysbase, {'Admin Port': 10200+7}) as asys:
            addTens = [ asys.createActor(AddTen) for x in range(0, 50, 10) ]
            for (frmA, toA) in zip(addTens, addTens[1:]):
                asys.tell(frmA, (1, toA))
            drop = asys.createActor(DropResults)
            asys.tell(drop, addEvery)  # verify it can be used
            import random
            for num in range(random.randint(0, 100)):
                start = random.choice(addTens)
                asys.tell(start, addEvery(asker=drop))  # drop results
            asys.tell(addTens[-1], addEvery(asker=drop))  # drop results
            sum = asys.ask(addTens[0], addEvery())
            self.assertEqual(sum.value, 50)
            for killA in addTens:
                asys.tell(killA, ActorExitRequest())


class TestMultiprocUDPSystem(TestASimpleSystem):
    testbase='MultiprocUDP'
    sysbase = 'multiprocUDPBase'

class TestMultiprocTCPSystem(TestASimpleSystem):
    testbase='MultiprocTCP'
    sysbase = 'multiprocTCPBase'

class TestMultiprocQueueSystem(TestASimpleSystem):
    testbase='MultiprocQueue'
    sysbase = 'multiprocQueueBase'

    unstable = 1  # Under high stress conditions, MultiprocQueue
                  # actors seem to get stuck in the internals of the
                  # multiprocess.Queue infrastructure.  There are
                  # other indications (both internal to Thespian and
                  # externally) that there may be some issues with
                  # Queue.

    def test07_LotsOfActorsEveryTenWithBackground(self): pass

class TestMultiprocQueueSystemUnstable(TestASimpleSystem):
    testbase='MultiprocQueue'
    unstable = 1
    sysbase = 'multiprocQueueBase'
    def test01_TenEvery(self): pass
    def test02_TenEvens(self): pass
    def test03_LotsOfActorsEvery(self): pass
    def test04_LotsOfActorsEveryTen(self): pass
    def test05_UniqueAddresses(self): pass
    def test06_TenEveryTen(self): pass
