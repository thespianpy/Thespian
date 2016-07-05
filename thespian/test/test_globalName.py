import logging
from thespian.actors import *
from thespian.test import *

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


class TestFuncGlobalName(object):

    def testPrimarySingletons(self, asys):
        one = asys.createActor(ThereCanBeOnlyOne, globalName = 'OnlyOne')
        assert 'ONE: yes' == asys.ask(one, 'yes')
        uno = asys.createActor(ThereCanBeOnlyOne, globalName = 'OnlyOne')
        assert str(one) == str(uno)

        assert 'ONE: end' == asys.ask(uno, 'end')

        # Different global name is different actor
        dos = asys.createActor(ThereCanBeOnlyOne, globalName = 'not the one')
        assert str(one) != str(dos)
        assert str(dos) != str(uno)
        assert 'ONE: time' == asys.ask(uno, 'time')
        assert 'ONE: day' == asys.ask(dos, 'day')

        # No global name is different actor
        tres = asys.createActor(ThereCanBeOnlyOne)
        assert str(uno) != str(tres)
        assert str(tres) != str(dos)
        assert 'ONE: year' == asys.ask(uno, 'year')
        assert 'ONE: of these days' == asys.ask(dos, 'of these days')
        assert 'ONE: Alice!' == asys.ask(tres, 'Alice!')

    def testSubActorsSingletons(self, asys):
        pa = asys.createActor(Parent)
        assert "PARENT: me" == asys.ask(pa, "me")

        subUno = asys.ask(pa, "newGlobalChild")
        assert pa != subUno
        assert "PARENT: me" == asys.ask(pa, "me")
        assert "ONE: me" == asys.ask(subUno, "me")
        
        subDos = asys.ask(pa, "newGlobalChild")
        assert str(subDos) == str(subUno)
        assert "PARENT: me" == asys.ask(pa, "me")
        assert "ONE: me" == asys.ask(subUno, "me")
        assert "ONE: again" == asys.ask(subDos, "again")

        subTres = asys.ask(pa, "newChild")
        assert subUno != subTres
        assert subTres != subUno
        assert "PARENT: me" == asys.ask(pa, "me")
        assert "ONE: me" == asys.ask(subUno, "me")
        assert "ONE: again" == asys.ask(subDos, "again")
        assert "PARENT: not me" == asys.ask(subTres, "not me")

    def testPrimaryAndSubActorSingletons(self, asys):
        pa = asys.createActor(Parent)
        subUno = asys.ask(pa, "newGlobalChild")
        # Now create primary with this name;  will not be the requested ActorClass
        dos = asys.createActor(Parent, globalName = "OnlyOne")
        assert str(subUno) == str(dos)
        assert "ONE: check" == asys.ask(subUno, "check")
        assert "ONE: balance" == asys.ask(dos, "balance")

    def testSubActorAndPrimarySingletons(self, asys):
        pa = asys.createActor(Parent)
        uno = asys.createActor(ThereCanBeOnlyOne, globalName = "OnlyOne")
        # Now create subActor with this name
        subDos = asys.ask(pa, "newGlobalChild")
        assert uno == subDos
        assert "ONE: check" == asys.ask(uno, "check")
        assert "ONE: balance" == asys.ask(subDos, "balance")

