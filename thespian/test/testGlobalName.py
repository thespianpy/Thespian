import unittest
import logging
import thespian.test.helpers
from thespian.actors import *
from thespian.test import ActorSystemTestCase

class ThereCanBeOnlyOne(Actor):
    def receiveMessage(self, msg, sender):
        self.send(sender, "ONE: %s"%msg)

class Parent(Actor):
    def receiveMessage(self, msg, sender):
        if msg == 'newChild':
            self.send(sender, self.createActor(Parent))
        elif msg == 'newGlobalChild':
            self.send(sender, self.createActor(ThereCanBeOnlyOne, globalName = 'OnlyOne'))
        else:
            self.send(sender, "PARENT: %s"%msg)


class TestASimpleSystem(ActorSystemTestCase):
    testbase='Simple'
    scope='func'

    def testPrimarySingletons(self):
        one = ActorSystem().createActor(ThereCanBeOnlyOne, globalName = 'OnlyOne')
        self.assertEqual('ONE: yes', ActorSystem().ask(one, 'yes'))
        uno = ActorSystem().createActor(ThereCanBeOnlyOne, globalName = 'OnlyOne')
        self.assertEqual(str(one), str(uno))

        self.assertEqual('ONE: end', ActorSystem().ask(uno, 'end'))

        # Different global name is different actor
        dos = ActorSystem().createActor(ThereCanBeOnlyOne, globalName = 'not the one')
        self.assertNotEqual(str(one), str(dos))
        self.assertNotEqual(str(dos), str(uno))
        self.assertEqual('ONE: time', ActorSystem().ask(uno, 'time'))
        self.assertEqual('ONE: day', ActorSystem().ask(dos, 'day'))

        # No global name is different actor
        tres = ActorSystem().createActor(ThereCanBeOnlyOne)
        self.assertNotEqual(str(uno), str(tres))
        self.assertNotEqual(str(tres), str(dos))
        self.assertEqual('ONE: year', ActorSystem().ask(uno, 'year'))
        self.assertEqual('ONE: of these days', ActorSystem().ask(dos, 'of these days'))
        self.assertEqual('ONE: Alice!', ActorSystem().ask(tres, 'Alice!'))

    def testSubActorsSingletons(self):
        pa = ActorSystem().createActor(Parent)
        self.assertEqual("PARENT: me", ActorSystem().ask(pa, "me"))

        subUno = ActorSystem().ask(pa, "newGlobalChild")
        self.assertNotEqual(pa, subUno)
        self.assertEqual("PARENT: me", ActorSystem().ask(pa, "me"))
        self.assertEqual("ONE: me", ActorSystem().ask(subUno, "me"))
        
        subDos = ActorSystem().ask(pa, "newGlobalChild")
        self.assertEqual(str(subDos), str(subUno))
        self.assertEqual("PARENT: me", ActorSystem().ask(pa, "me"))
        self.assertEqual("ONE: me", ActorSystem().ask(subUno, "me"))
        self.assertEqual("ONE: again", ActorSystem().ask(subDos, "again"))

        subTres = ActorSystem().ask(pa, "newChild")
        self.assertNotEqual(subUno, subTres)
        self.assertNotEqual(subTres, subUno)
        self.assertEqual("PARENT: me", ActorSystem().ask(pa, "me"))
        self.assertEqual("ONE: me", ActorSystem().ask(subUno, "me"))
        self.assertEqual("ONE: again", ActorSystem().ask(subDos, "again"))
        self.assertEqual("PARENT: not me", ActorSystem().ask(subTres, "not me"))

    def testPrimaryAndSubActorSingletons(self):
        pa = ActorSystem().createActor(Parent)
        subUno = ActorSystem().ask(pa, "newGlobalChild")
        # Now create primary with this name;  will not be the requested ActorClass
        dos = ActorSystem().createActor(Parent, globalName = "OnlyOne")
        self.assertEqual(str(subUno), str(dos))
        self.assertEqual("ONE: check", ActorSystem().ask(subUno, "check"))
        self.assertEqual("ONE: balance", ActorSystem().ask(dos, "balance"))

    def testSubActorAndPrimarySingletons(self):
        pa = ActorSystem().createActor(Parent)
        uno = ActorSystem().createActor(ThereCanBeOnlyOne, globalName = "OnlyOne")
        # Now create subActor with this name
        subDos = ActorSystem().ask(pa, "newGlobalChild")
        self.assertEqual(uno, subDos)
        self.assertEqual("ONE: check", ActorSystem().ask(uno, "check"))
        self.assertEqual("ONE: balance", ActorSystem().ask(subDos, "balance"))


class TestMultiprocUDPSystem(TestASimpleSystem):
    testbase='MultiprocUDP'
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

