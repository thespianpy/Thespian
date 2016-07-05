"""This test creates two top level actors and one sub-actor and
   verifies that the actors can exchange sequences of messages."""

import time
from thespian.actors import *
from thespian.test import *

class rosaline(Actor):
    name = 'Rosaline'

class Romeo(Actor):
    def receiveMessage(self, msg, sender):
        if isinstance(msg, JulietAppears):
            self.send(msg.juliet, "But, soft! what light through yonder window breaks?")
        elif isinstance(msg, ActorExitRequest):
            pass  # nothing special, just die
        elif msg == 'Ay me!':
            self.send(sender, 'She speaks!')
        elif msg == 'O Romeo, Romeo! wherefore art thou Romeo?':
            self.send(sender, 'Shall I hear more, or shall I speak at this?')
        elif 'rose' in msg:
            pass # wait for it
        elif 'sweet' in msg:
            self.send(sender, 'Like softest music to attending ears!')
        elif 'hello' in msg:
            print('Hello from %s'%(str(self)))
        elif 'who_are_you' == msg:
            self.send(sender, self.myAddress)
        # otherwise sit and swoon


class Capulet(Actor):
    def receiveMessage(self, msg, sender):
        if msg == "has a daughter?":
            self.send(sender, self.createActor(Juliet))


class Juliet(Actor):
    def __init__(self, *args, **kw):
        self.nurse = None
        self.recalled = False
        super(Juliet, self).__init__(*args, **kw)
    def receiveMessage(self, msg, sender):
        if isinstance(msg, ActorExitRequest):
            pass  # nothing special, just die
        elif "what light" in msg:
            self.send(sender, 'Ay me!')
        elif msg == 'She speaks!':
            self.send(sender, 'O Romeo, Romeo! wherefore art thou Romeo?')
        elif msg == 'Shall I hear more, or shall I speak at this?':
            self.send(sender, "What's in a name? That which we call a rose")
            self.send(sender, "By any other name would smell as sweet")
        elif msg == 'Like softest music to attending ears!':
            if self.nurse:
                self.send(self.nurse, 'Anon, good nurse!')
            else:
                self.recalled = True
        elif msg == 'Mistress!':
            self.nurse = sender
            if self.recalled:
                self.send(self.nurse, 'Anon, good nurse!')
        elif 'who_are_you' == msg:
            self.send(sender, self.myAddress)


class Nurse(Actor):
    def __init__(self, *args, **kw):
        self.heardItAll = False
        super(Nurse, self).__init__(*args, **kw)
    def receiveMessage(self, msg, sender):
        if type(msg) == type((1,2)) and msg[0] == 'begin':
            self.send(msg[1], JulietAppears(msg[2]))
            self.send(msg[2], 'Mistress!')
        elif msg == 'Anon, good nurse!':
            self.heardItAll = True
        elif msg == 'done?':
            self.send(sender, 'Fini' if self.heardItAll else 'not yet')


class JulietAppears:
    stage = 'Right'
    def __init__(self, julietAddr):
        self.juliet = julietAddr


class TestFuncActors():


    def test01_ActorSystemStartupShutdown(self, asys):
        rosalineA = asys.createActor(rosaline)
        # just finish, make sure no exception is thrown.

    def test01_1_ActorSystemMultipleShutdown(self, asys):
        rosalineA = asys.createActor(rosaline)
        asys.shutdown()
        asys.shutdown()

    def test02_PrimaryActorCreation(self, asys):
        romeo = asys.createActor(Romeo)
        juliet = asys.createActor(Juliet)
        assert romeo != juliet

    def test03_CreateActorUniqueAddress(self, asys):
        romeo = asys.createActor(Romeo)
        juliet = asys.createActor(Juliet)
        assert romeo != juliet
        romeo2 = asys.createActor(Romeo)
        assert romeo != romeo2

    def NOtest04_PossibleActorSystemResourceExhaustion(self):
        try:
            addresses = [asys.createActor(Juliet) for n in range(10000)]
        except OSError as err:
            import errno
            if err.errno == errno.EGAIN:
                pass
            else:
                raise


    def test05_ManyActorsUniqueAddress(self, asys):
        addresses = [asys.createActor(Juliet) for n in range(50)]
        uniqueAddresses = []
        duplicates = []
        for A in addresses:
            if A in uniqueAddresses:
                duplicates.append(A)
            else:
                uniqueAddresses.append(A)
        if len(addresses) != len(uniqueAddresses):
            print('Duplicates: %s'%map(str, duplicates))
            if duplicates:
                for each in duplicates:
                    print('... %s at: %s'%(str(each), str([N for N,A in enumerate(addresses) if A == each])))
            print('Note: if this is a UDPTransport test, be advised that Linux occasionally does seem to assign the same UDP port multiple times.  Linux bug?')
        assert len(addresses) == len(uniqueAddresses)

    def test06_ManyActorsValidAddresses(self, asys):
        import string
        addresses = [asys.createActor(Juliet) for n in range(100)]
        for addr in addresses:
            invchar = ''.join([c for c in str(addr)
                               if c not in string.ascii_letters + string.digits + "-~/():., '|>"])
            assert str(addr) == str(addr) + invchar  # invchar should be blank

    def test07_SingleNonListeningActorTell(self, asys):
        rosalineA = asys.createActor(rosaline)
        # rosaline does not override the receiveMessage method, so the
        # Actor default method will throw an exception.  This will
        # Kill the rosaline Actor.  It's a top level Actor, so it will
        # not be restarted.  This will cause the 'hello' message to be
        # delivered to the DeadLetterBox.  Verify that no exception
        # makes its way out of the ActorSystem here.
        asys.tell(rosalineA, 'hello')
        assert True

    def test08_SingleActorTell(self, asys):
        romeoA = asys.createActor(Romeo)
        asys.tell(romeoA, 'hello')
        # Nothing much happens, Romeo is smitten and has no time for trivialities, but
        # he will try to generate str() of himself.

    def test09_SingleActorAsk(self, asys):
        romeoA = asys.createActor(Romeo)
        resp = asys.ask(romeoA, 'O Romeo, Romeo! wherefore art thou Romeo?', 1)
        assert resp, 'Shall I hear more == or shall I speak at this?'

    def test10_ActorAskWithNoResponse(self, asys):
        romeoA = asys.createActor(Romeo)
        # This test is possibly unique to the simpleSystemBase, which
        # will run an process all messages on an ask (or tell) call.
        # Properly there is no way to determine if an answer is
        # forthcoming from an asynchronous system, so all this can do
        # is assert that there is no response within a particular time
        # period.  At this point, timing is not supported, so this
        # test is underspecified and assumptive.
        resp = asys.ask(romeoA, "What's in a name? That which we call a rose", 1.5)
        assert resp is None
        # Now verify that the Actor and system are still alive and operating normally.
        resp = asys.ask(romeoA, 'O Romeo, Romeo! wherefore art thou Romeo?', 1)
        assert resp, 'Shall I hear more == or shall I speak at this?'

    def test11_SingleActorAskMultipleTimes(self, asys):
        romeoA = asys.createActor(Romeo)
        r = asys.ask(romeoA, 'O Romeo, Romeo! wherefore art thou Romeo?', 1)
        assert r == 'Shall I hear more, or shall I speak at this?'
        r = asys.ask(romeoA, 'O Romeo, Romeo! wherefore art thou Romeo?', 1)
        assert r == 'Shall I hear more, or shall I speak at this?'
        r = asys.ask(romeoA, 'Ay me!', 1)
        assert r == 'She speaks!'
        r = asys.ask(romeoA, 'O Romeo, Romeo! wherefore art thou Romeo?', 1)
        assert r == 'Shall I hear more, or shall I speak at this?'

    def test12_MultipleActorsAskMultipleTimes(self, asys):
        romeo = asys.createActor(Romeo)
        r = asys.ask(romeo, 'O Romeo, Romeo! wherefore art thou Romeo?', 1)
        assert r == 'Shall I hear more, or shall I speak at this?'
        juliet = asys.createActor(Juliet)
        r = asys.ask(romeo, 'O Romeo, Romeo! wherefore art thou Romeo?', 1)
        assert r == 'Shall I hear more, or shall I speak at this?'
        r = asys.ask(romeo, 'Ay me!', 1)
        assert r == 'She speaks!'
        r = asys.ask(juliet, 'She speaks!', 1)
        assert r == 'O Romeo, Romeo! wherefore art thou Romeo?'
        r = asys.ask(romeo, 'Ay me!', 1)
        assert r == 'She speaks!'
        r = asys.ask(juliet, "Do you know what light that is?", 1)
        assert r == 'Ay me!'

    def test13_SubActorCreation(self, asys):
        capulet = asys.createActor(Capulet)
        juliet = asys.ask(capulet, 'has a daughter?', 2.5)
        print ('Juliet is: %s'%str(juliet))
        assert juliet is not None
        if juliet:
            r = asys.ask(juliet, 'what light?')
            assert r == 'Ay me!', 0.75
            juliet2 = asys.ask(capulet, 'has a daughter?', 1)
            assert juliet2 is not None
            if juliet2:
                r = asys.ask(juliet2, 'what light?', 0.5)
                assert r == 'Ay me!'
            r = asys.ask(juliet, 'what light?', 0.5)
            assert r == 'Ay me!'

    def test14_EntireActWithActorStart(self, asys):
        romeo = asys.createActor(Romeo)
        juliet = asys.createActor(Juliet)
        nurse = asys.createActor(Nurse)
        assert asys.ask(nurse, 'done?', 1) == 'not yet'
        asys.tell(nurse, ('begin', romeo, juliet))

        for X in range(50):
            if asys.ask(nurse, 'done?', 1) == 'Fini':
                break
            time.sleep(0.01)  # Allow some time for the entire act
        r = asys.ask(nurse, 'done?', 1)
        assert r == 'Fini'

    def test15_IncompleteActMissingActor(self, asys):
        romeo = asys.createActor(Romeo)
        juliet = asys.createActor(Juliet)
        # no nurse actor created
        asys.tell(romeo, JulietAppears(juliet))
        # No error should occur here when Juliet reaches the end and
        # doesn't have a nurse to tell.

        time.sleep(0.05)  # Allow some time for the entire act

        # Now create the nurse and tell her to talk to romeo and
        # juliet, which should cause completion
        nurse = asys.createActor(Nurse)
        r = asys.ask(nurse, 'done?', 1)
        assert r == 'not yet'
        asys.tell(nurse, ('begin', romeo, juliet))

        for X in range(50):
            if asys.ask(nurse, 'done?', 1) == 'Fini':
                break
            time.sleep(0.01)  # Allow some time for the entire act
        r = asys.ask(nurse, 'done?', 1)
        assert r == 'Fini'

    def test16_ActorProperties(self, asys):
        romeo = asys.createActor(Romeo)
        juliet = asys.createActor(Juliet)

        r = asys.ask(romeo, 'who_are_you', 0.25)
        assert r is not None
        r = asys.ask(juliet, 'who_are_you', 0.25)
        assert r is not None
        r1 = asys.ask(romeo, 'who_are_you', 0.25)
        r2 = asys.ask(juliet, 'who_are_you', 0.25)
        assert r1 != r2
