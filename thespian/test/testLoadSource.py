import unittest
from thespian.test import ActorSystemTestCase, simpleActorTestLogging
import time
import thespian.test.helpers
from thespian.actors import *
import zipfile
import tempfile
import os, sys
import copy



def _encryptROT13Zipfile(zipFname):
    "Encrypts a zipfile on disk into a new file with ROT13 encryption of contents"
    zFile = open(zipFname, 'rb')
    zData = zFile.read()
    zFile.close()
    efName = zipFname + '.enc'
    zEFile = open(efName, 'wb')
    if zData:
        if isinstance(zData[0], int):
            rot13 = lambda b: (b + 13) % 256
            hdr = b'ROT13___'
            join = bytes
        else:
            rot13 = lambda b: chr((ord(b) + 13) % 256)
            hdr = 'ROT13___'
            join = ''.join
        z = hdr + join(map(rot13, zData))
        try:
            zEFile.write(z)
        except TypeError:
            zEFile.write(bytes(z, 'UTF-8'))
    zEFile.close()
    return efName


def _decryptROT13(encdata):
    "Converts input bytes read from a file into ROT13 decrypted bytes"
    if not encdata: return None
    if isinstance(encdata[0], int):
        unrot13 = lambda b: (b + 256 - 13) % 256
        join = bytes
    else:
        unrot13 = lambda b: chr((ord(b) + 256 - 13) % 256)
        join = ''.join
    if encdata[:8].decode() != 'ROT13___':
        return None
    clear = join(map(unrot13, encdata[8:]))
    return clear


fooSource = '''
from thespian.actors import Actor, requireCapability, PoisonMessage
@requireCapability('Foo Allowed')
class FooActor(Actor):
    def receiveMessage(self, msg, sender):
        if type(msg) == type(1.0):
            self.send(sender, msg + 3.8)
        elif type(msg) == type(""):
            # Some import tests
            from frog import Frog
            import toad
            self.send(sender, 'GOT: '+Frog(toad.Toad(msg)))
        elif type(msg) == type(1):
            # Some more import tests
            from barn.cow.moo import cow_says  # Import from within this hashSource
            import calendar   # Import a regular module that has not been imported before
            self.send(sender, 'COW: ' + str(cow_says()) + ' on %s'%calendar.weekday(2001,9,11))
        elif type(msg) == type((1,2)):
            if not hasattr(self, 'subA'):
                self.subA = self.createActor('barn.cow.moo.MooActor')
            self.send(self.subA, (msg[1], sender))
        elif isinstance(msg, PoisonMessage) and type(msg.poisonMessage) == type((1,2)):
            self.send(msg.poisonMessage[1], 'FAILED (poisonous)')
        elif type(msg) == type([1,2]):
            inAWorld = self.createActor(Narrator, msg[0])
            self.send(inAWorld, (msg[1], sender))


class Narrator(Actor):
    @staticmethod
    def actorSystemCapabilityCheck(capabilities, requirements):
         return all([requirements[R] == capabilities[R] for R in requirements])
    def receiveMessage(self, msg, sender):
        if type(msg) == type( (1,2) ):
            self.send(msg[1], 'In a WORLD: ' + str(msg[0]))
'''

frogSource = 'def Frog(arg): return str(arg)'
toadSource = 'def Toad(arg): return str(arg)'

mooSource = '''
from thespian.actors import Actor, requireCapability
from frog import Frog
import toad

@requireCapability('Cows Allowed')
class MooActor(Actor):
    def receiveMessage(self, msg, sender):
        if type(msg) == type(""):
            self.send(sender, 'Moo: '+toad.Toad(Frog(msg)))
        elif type(msg) == type( (1,2) ):
            self.send(msg[1], 'And MOO: ' + str(msg[0]))

def cow_says():
    return 'Moooo'
'''

dogSource = '''
from thespian.actors import Actor, requireCapability
@requireCapability('Dogs Allowed')
class DogActor(Actor):
    def receiveMessage(self, msg, sender):
        if type(msg) == type(""):
            self.send(sender, 'Woof! '+str(msg))
        elif type(msg) == type( (1,2) ):
            self.send(msg[1], ('Ruff Ruff: ' + str(msg[0]), sender))
'''

# Pig exercises absolute imports
pigSource = '''
from thespian.actors import Actor
from barn.chicken import Cluck
from . import goose
from barn.cow.moo import cow_says
from frog import Frog
import toad
class PigActor(Actor):
    def receiveMessage(self, msg, sender):
        if type(msg) == type(""):
            self.send(sender, 'Oink ' + toad.Toad(Frog(Cluck(goose.Honk(msg)))) + ' ' + cow_says())
'''

# Sow exercises relative imports
sowSource = '''
from thespian.actors import Actor
from .chicken import Cluck
import barn
import barn.goose
from .cow.moo import cow_says
from frog import Frog
import toad
class SowActor(Actor):
    def receiveMessage(self, msg, sender):
        if type(msg) == type(""):\r
            self.send(sender, cow_says() + ' Oink ' + Cluck(Frog(toad.Toad(barn.goose.Honk(msg)))))
                '''  # <-- unexpected indentation without a trailing newline.

# Piglet exercises OLD-style relative imports (valid in 2.x, but not 3.x)
pigletSource = '''
from thespian.actors import Actor
from chicken import Cluck
import barn
import goose
from cow.moo import cow_says
from frog import Frog
import toad
class PigletActor(Actor):
    def receiveMessage(self, msg, sender):
        if type(msg) == type(""):\r
            self.send(sender, cow_says() + ' Oink ' + Cluck(Frog(toad.Toad(goose.Honk(msg)))))
                '''  # <-- unexpected indentation without a trailing newline.

chickenSource = 'def Cluck(msg): return "Cluck " + msg'
roosterSource = 'def Crow(msg): return "Cock-a-doodle-doo " + msg'
gooseSource = '''
def Honk(msg): return "Honk " + msg
import sys'''  # <-- no terminating newline

barnInitSource = '''
import sys
if sys.version_info >= (3,):
    from .rooster import *
else:
    import rooster
    from .chicken import *
'''


class BarActor(Actor):
    def receiveMessage(self, msg, sender):
        if type(msg) == type(""):
            self.send(sender, 'SAW: '+str(msg))


class CreateTestSourceZips(object):
    def createZips(self):
        self.tmpdir = tempfile.mkdtemp()

        self.foozipFname = os.path.join(self.tmpdir, 'foosrc.zip')
        foozip = zipfile.ZipFile(self.foozipFname, 'w')
        foozip.writestr('__init__.py', '')
        foozip.writestr('foo.py', fooSource)
        foozip.writestr('frog.py', frogSource)
        foozip.writestr('toad.py', toadSource)
        foozip.writestr('barn/__init__.py', barnInitSource)
        foozip.writestr('barn/pig.py', pigSource)
        foozip.writestr('barn/chicken.py', chickenSource)
        foozip.writestr('barn/rooster.py', roosterSource)
        foozip.writestr('barn/goose.py', gooseSource)
        foozip.writestr('barn/sow.py', sowSource)
        foozip.writestr('barn/piglet.py', pigletSource)
        foozip.writestr('barn/cow/__init__.py', '')
        foozip.writestr('barn/cow/moo.py', mooSource)
        foozip.close()

        self.foozipEncFile = _encryptROT13Zipfile(self.foozipFname)

        self.dogzipFname = os.path.join(self.tmpdir, 'dogsrc.zip')
        dogzip = zipfile.ZipFile(self.dogzipFname, 'w')
        dogzip.writestr('dog.py', dogSource)
        dogzip.close()

        self.dogzipEncFile = _encryptROT13Zipfile(self.dogzipFname)

    def removeZips(self):
        import shutil
        if os.path.exists(self.tmpdir): shutil.rmtree(self.tmpdir)


class TestRoundTripROT13(unittest.TestCase, CreateTestSourceZips):
    scope='unit'

    def setUp(self):
        self.createZips()

    def tearDown(self):
        self.removeZips()

    def test_simple_rot13_enc_dec(self):
        self.simplezipFname = os.path.join(self.tmpdir, 'simple.zip')
        zf = open(self.simplezipFname, 'wb')
        zf.write(b'abcdABCD1234')
        zf.close()
        encfname = _encryptROT13Zipfile(self.simplezipFname)
        encdata = open(encfname, 'rb').read()
        decdata = _decryptROT13(encdata)
        self.assertEqual(decdata, b'abcdABCD1234')

    def test_rot13_enc_dec(self):
        encf = open(self.foozipEncFile, 'rb')
        encdata = encf.read()
        encf.close()
        foozipDecoded = _decryptROT13(encdata)
        from io import BytesIO
        from zipfile import ZipFile
        foozip = ZipFile(BytesIO(foozipDecoded))

        names = foozip.namelist()
        self.assertEqual(names[0], '__init__.py')
        self.assertEqual(names[-1], 'barn/cow/moo.py')
        self.assertEqual(len(names), 13)


class TestDirectZipfile(unittest.TestCase):
    # Note that these tests try to import from the zipfile directly,
    # which can pollute the local namespace.  However, these tests are
    # useful to ensure that the contents of the zipfile are compatible
    # with the current python interpreter/version and that therefore
    # the thespian importing has a reasonable chance of success.  By
    # scoping this test as unit scope (which it is) and the thespian
    # importing tests as func scope, they do not get run in the same
    # python invocation and so there is no leakage between the two.

    # The tricky part is that sys.modules is global and once a module
    # is imported, it expects to be able to find related imports in
    # that same source, so the zipfile used for the first import must
    # remain available even though there are multiple tests.  To solve
    # this, there is a class-level ZipManager instance that creates
    # them, and testzzzzzz to delete them (where that test should come
    # last in alphabetical sorting... running tests in random orders
    # will defeat this and probably cause test failures.  Running
    # specific tests only will leave behind zipfiles in $TMPDIR).

    scope='unit'

    class ZipManager(CreateTestSourceZips):
        def __init__(self):
            self.createZips()
            self.origpath = copy.deepcopy(sys.path)

        def remove(self):
            if hasattr(self, 'origpath'):
                self.removeZips()
                sys.path = self.origpath
                del self.origpath

        def __del__(self): self.remove()

    def setUp(self):
        if not hasattr(self.__class__, 'zips'):
            self.__class__.zips = self.__class__.ZipManager()
        self.foozipFname = self.__class__.zips.foozipFname

    def testzzzzzz(self):
        self.__class__.zips.remove()

    def testFooString(self):
        sys.path.insert(0, self.foozipFname)
        # First try to import foo itself from the zipfile
        import foo
        f = foo.FooActor()
        # calling the FooActor with a string causes some additional
        # non-module-level imports to occur.  If it makes it past the
        # imports, it will try to send back a response which will fail
        # since we are using the invalid address of "sender".
        self.assertRaises(InvalidActorAddress,
                          f.receiveMessage, "hi", "sender")

    def testFooInteger(self):
        sys.path.insert(0, self.foozipFname)
        # First try to import foo itself from the zipfile
        import foo
        f = foo.FooActor()
        # calling the FooActor with an integer causes some additional
        # non-module-level imports to occur that are *different* than
        # the ones that occur when it gets passed a string.  If it
        # makes it past the imports, it will try to send back a
        # response which will fail since we are using the invalid
        # address of "sender".
        self.assertRaises(InvalidActorAddress,
                          f.receiveMessage, 5, "sender")

    def testPig(self):
        sys.path.insert(0, self.foozipFname)
        # First try to import barn top level and pig from the zipfile
        import barn.pig
        f = barn.pig.PigActor()
        # calling the PigActor with a string causes some additional
        # non-module-level imports to occur.
        self.assertRaises(InvalidActorAddress,
                          f.receiveMessage, "what", "sender")

    def testSow(self):
        sys.path.insert(0, self.foozipFname)
        # First try to import barn top level and sow from the zipfile
        import barn.sow
        f = barn.sow.SowActor()
        # calling the SowActor with a string causes some additional
        # non-module-level imports to occur.
        self.assertRaises(InvalidActorAddress,
                          f.receiveMessage, "what", "sender")

    def testPiglet(self):
        if sys.version_info < (3,0):
            sys.path.insert(0, self.foozipFname)
            # First try to import barn top level and piglet from the zipfile
            import barn.piglet
            f = barn.piglet.PigletActor()
            # calling the PigletActor with a string causes some additional
            # non-module-level imports to occur.
            self.assertRaises(InvalidActorAddress,
                              f.receiveMessage, "what", "sender")


class TestASimpleSystem(unittest.TestCase, CreateTestSourceZips):
    testbase='Simple'
    scope='func'
    actorSystemBase = 'simpleSystemBase'
    portBase = 0

    def setUp(self):
        self.createZips()
        self.systems = {}

    def startSystems(self, portOffset):
        # Only define base capabilities, not extended capabilities
        self.capabilities = { 'One': { 'Admin Port': 30001 + portOffset + self.portBase,
                                       'Foo Allowed': True,
                                       'Cows Allowed': True,
                                       'Dogs Allowed': True,
                                   },
                          }
        for each in ['One']:  # 'One' must be first
            self.systems[each] = ActorSystem(self.actorSystemBase, self.capabilities[each],
                                             logDefs = simpleActorTestLogging(),
                                             transientUnique = True)
        time.sleep(0.1)  # Wait for Actor Systems to start

    def tearDown(self):
        for each in self.systems:
            self.systems[each].shutdown()
        self.removeZips()

    def test00_systemsRunnable(self):
        self.startSystems(0)

    def test01_verifyFooActorNotAvailableByName(self):
        self.startSystems(10)
        self.assertRaises(ImportError, self.systems['One'].createActor, 'foo.FooActor')
        bar = self.systems['One'].createActor('thespian.test.testLoadSource.BarActor')
        self.assertEqual('SAW: hello', self.systems['One'].ask(bar, 'hello', 1))

    def test01_verifyFooActorNotAvailableWithBogusSourceHash(self):
        self.startSystems(20)
        self.assertRaises(InvalidActorSourceHash,
                          self.systems['One'].createActor,
                          'foo.FooActor', sourceHash = 'this is bogus')

    def test01_verifyloadSourceHandlesBadFilename(self):
        self.startSystems(30)
        self.assertRaises(IOError,
                          self.systems['One'].loadActorSource, 'bad file name here')


    def _loadFooSource(self):
        srchash = self.systems['One'].loadActorSource(self.foozipFname)
        self.assertIsNotNone(srchash)
        return srchash

    def test02_verifyMainActorAvailableWhenLoaded(self):
        self.startSystems(40)
        srchash = self._loadFooSource()
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))

    def test02_verifySubActorAvailableWhenLoaded(self):
        self.startSystems(50)
        srchash = self._loadFooSource()
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('And MOO: great', self.systems['One'].ask(foo, ('discard', 'great'), 1))

    def test02_verifySubModuleAvailableWhenLoaded(self):
        self.startSystems(60)
        srchash = self._loadFooSource()
        cow = self.systems['One'].createActor('barn.cow.moo.MooActor', sourceHash=srchash)
        self.assertEqual('Moo: got milk', self.systems['One'].ask(cow, 'got milk', 1))

    def test02_verifyAllAbsoluteImportPossibilities(self):
        self.startSystems(70)
        srchash = self._loadFooSource()
        pig = self.systems['One'].createActor('barn.pig.PigActor', sourceHash=srchash)
        self.assertEqual('Oink Cluck Honk ready? Moooo', self.systems['One'].ask(pig, 'ready?', 1))

    def test02_verifyAllRelativeImportPossibilities(self):
        self.startSystems(80)
        srchash = self._loadFooSource()
        sow = self.systems['One'].createActor('barn.sow.SowActor', sourceHash=srchash)
        self.assertEqual('Moooo Oink Cluck Honk ready?', self.systems['One'].ask(sow, 'ready?', 1))

    def test02_verifyAllOLDSTYLERelativeImportPossibilities(self):
        if sys.version_info < (3,0):
            self.startSystems(85)
            srchash = self._loadFooSource()
            piglet = self.systems['One'].createActor('barn.piglet.PigletActor', sourceHash=srchash)
            self.assertEqual('Moooo Oink Cluck Honk ready?', self.systems['One'].ask(piglet, 'ready?', 1))

    def test02_verifyHashSourceAvailablePostLoadFromMembers(self):
        self.startSystems(90)
        srchash = self._loadFooSource()
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        # Cause a load-on-demand of a new module (in the loaded
        # sources) from the loaded sources themselves and ensure it's still available.
        self.assertEqual('COW: Moooo on 1', self.systems['One'].ask(foo, 1))

    def test02_verifyHashSourceNotInGlobalNamespace(self):
        self.startSystems(100)
        srchash = self._loadFooSource()
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        # Verify loaded source is not accessible globally
        try:
            from bar.cow.moo import cow_says
            self.assertTrue(False)  # should never get here
        except ImportError:
            self.assertTrue(True)   # want this
        except Exception:
            self.assertTrue(False)  # but not these


    # Note: Perform these after the successful load tests (test02_) to
    # ensure that the modules loaded by those tests are no longer
    # available in the namespace to cause these tests (test03_) to fail.
    def test03_verifyFooActorNotAvailableWithoutModuleQualifiersOrHash(self):
        self.startSystems(110)
        srchash = self._loadFooSource()
        self.assertRaises(InvalidActorSpecification,
                          self.systems['One'].createActor, 'FooActor')

    def test03_verifyFooActorNotAvailableWithoutCorrectHash(self):
        self.startSystems(120)
        # No sourceHash specified, so the module foo is searched for
        # in the standard search path.
        srchash = self._loadFooSource()
        self.assertRaises(ImportError, self.systems['One'].createActor, 'foo.FooActor')

    def test03_verifyFooActorNotAvailableWithoutModuleQualifiers(self):
        self.startSystems(130)
        srchash = self._loadFooSource()
        # The FooActor is not available without module qualifiers even if proper hash is specified
        self.assertRaises(InvalidActorSpecification,
                          self.systems['One'].createActor, 'FooActor',
                          sourceHash = srchash)

    def test04_verifyReloadOfChangedModuleAllowsBothToExistSimultaneously(self):
        self.startSystems(140)
        srchash = self._loadFooSource()
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('And MOO: great', self.systems['One'].ask(foo, ('discard', 'great')))

        # Update the foo sources
        foo2zipFname = os.path.join(self.tmpdir, 'foo2src.zip')
        foozip = zipfile.ZipFile(foo2zipFname, 'w')
        foozip.writestr('foo.py', fooSource.replace('GOT:', 'TOG:'))
        foozip.writestr('barn/__init__.py', '')
        foozip.writestr('barn/cow/__init__.py', '')
        foozip.writestr('barn/cow/moo.py', mooSource.replace('And MOO:', '& MOO:'))
        foozip.writestr('frog.py', frogSource)
        foozip.writestr('toad.py', toadSource)
        foozip.writestr('__init__.py', '')
        foozip.close()

        # Load the updated foo sources... next to the original
        srchash2 = self.systems['One'].loadActorSource(foo2zipFname)
        self.assertIsNotNone(srchash2)
        self.assertNotEqual(srchash, srchash2)

        foo2 = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash2)
        self.assertEqual('TOG: good one', self.systems['One'].ask(foo2, 'good one', 1))
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('& MOO: great', self.systems['One'].ask(foo2, ('discard', 'great'), 1))
        self.assertEqual('And MOO: great', self.systems['One'].ask(foo, ('discard', 'great'), 1))

        self.assertEqual('COW: Moooo on 1', self.systems['One'].ask(foo, 1, 1))
        self.assertEqual('COW: Moooo on 1', self.systems['One'].ask(foo2, 1, 1))

    def test04_verifyMultipleSeparateModulesLoaded(self):
        self.startSystems(150)
        srchash = self._loadFooSource()
        srchash2 = self.systems['One'].loadActorSource(self.dogzipFname)
        self.assertIsNotNone(srchash2)
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        dog = self.systems['One'].createActor('dog.DogActor', sourceHash=srchash2)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('And MOO: great', self.systems['One'].ask(foo, ('discard', 'great')))
        self.assertEqual('Woof! bark', self.systems['One'].ask(dog, 'bark', 1))

    def test04_verifyMultipleSeparateModulesRequireCorrectSourceHashOnCreate(self):
        self.startSystems(160)
        srchash = self._loadFooSource()
        srchash2 = self.systems['One'].loadActorSource(self.dogzipFname)
        self.assertIsNotNone(srchash2)
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertRaises(ImportError, self.systems['One'].createActor,
                          'dog.DogActor', sourceHash=srchash)  # wrong source hash
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))

    def test05_verifyUnloadOfHashedSourcePreventsActorCreation(self):
        self.startSystems(170)
        srchash = self._loadFooSource()
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('And MOO: great', self.systems['One'].ask(foo, ('discard', 'great'), 1))
        # Unload fooSource
        self.systems['One'].unloadActorSource(srchash)
        # Test cannot create actors anymore
        self.assertRaises(InvalidActorSourceHash,
                          self.systems['One'].createActor,
                          'foo.FooActor', sourceHash = srchash)

    def test05_verifyUnloadOfHashedSourceDoesNotKillActiveActors(self):
        self.startSystems(180)
        srchash = self._loadFooSource()
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual(12.1, round(self.systems['One'].ask(foo, 8.3, 1), 2))
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('And MOO: great', self.systems['One'].ask(foo, ('discard', 'great'), 1))
        # Unload fooSource
        self.systems['One'].unloadActorSource(srchash)
        # Test foo actors still exist
        self.assertEqual(13.9, round(self.systems['One'].ask(foo, 10.1, 1), 2))
        # Note: behavioral difference here between ActorSystems using
        # the local process memory (e.g. simpleSystemBase) and
        # ActorSystems using remote processes (e.g. multiprocTCPBase,
        # multiprocUDPBase).  The former will have fully unloaded the
        # module and so the imports attempted by foo will fail,
        # whereas the latter affects only *new* Actor processes, but
        # not existing processes, so the existing processes will still have the imports available.
        if self.actorSystemBase in ['simpleSystemBase']:
            self.assertIsInstance(self.systems['One'].ask(foo, 'good one', 1), PoisonMessage)
        else:
            self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('And MOO: great', self.systems['One'].ask(foo, ('discard', 'great')))


    # Unloading an actor source has indeterminate effects on running
    # Actors.  For actors that are part of the current process
    # (e.g. simpleSystemBase, multi-threaded system bases) the unload
    # will probably make the source unavailable for running actors as
    # well.  For actors in a multi-process configuration, the unload
    # at the local/admin point will not likely affect running actors
    # (this would have to be implemented by propagating the unload to
    # all other actors, which is of questionable benefit compared to
    # the overhead and timing issue).  For this reason, the following
    # test is NOT performed.

    # def test05_verifyUnloadOfHashedSourceDoesNotAllowNewSubActorsToBeCreated(self):
    #     srchash = self._loadFooSource()
    #     foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
    #     self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
    #     # Unload fooSource
    #     self.systems['One'].unloadActorSource(srchash)
    #     # Verify cannot create sub-actor from still-existing actor from unloaded source

    #     # First, show that an attempt to create an actor that was in
    #     # the removed source gives back an actorAddress to the
    #     # creating Actor, but the actor is never created and thus the
    #     # ask will timeout.
    #     self.assertIsNone(self.systems['One'].ask(foo, ('discard', 'great'), 0.25))

    #     # Now show that an attempt to get a running actor to import a
    #     # module that has been unloaded will cause that Actor to fail
    #     # on the import, resulting in a PoisonMessage indication.
    #     r = self.systems['One'].ask(foo, 1, 0.25)
    #     self.assertIsInstance(r, PoisonMessage)
    #     self.assertEqual(r.poisonMessage, 1)

    def test05_verifyMultipleSeparateModulesCanUseOtherAfterFirstUnloaded(self):
        self.startSystems(190)
        srchash = self._loadFooSource()
        srchash2 = self.systems['One'].loadActorSource(self.dogzipFname)
        self.assertIsNotNone(srchash2)
        self.systems['One'].unloadActorSource(srchash)
        self.assertRaises(InvalidActorSourceHash,
                          self.systems['One'].createActor,
                          'foo.FooActor', sourceHash=srchash)
        dog = self.systems['One'].createActor('dog.DogActor', sourceHash=srchash2)
        self.assertEqual('Woof! bark', self.systems['One'].ask(dog, 'bark', 1))

    def test04_verifyReloadOfChangedModuleAndUnloadOfOriginal(self):
        self.startSystems(200)
        srchash = self._loadFooSource()
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('And MOO: great', self.systems['One'].ask(foo, ('discard', 'great')))

        # Update the foo sources
        from io import BytesIO
        zipdata = BytesIO()
        foozip = zipfile.ZipFile(zipdata, 'a')
        foozip.writestr('foo.py', fooSource.replace('GOT:', 'TOG:'))
        foozip.writestr('frog.py', frogSource)
        foozip.writestr('toad.py', toadSource)
        foozip.writestr('__init__.py', '')
        foozip.writestr('barn/__init__.py', '')
        foozip.writestr('barn/cow/__init__.py', '')
        foozip.writestr('barn/cow/moo.py', mooSource.replace('And MOO:', '& MOO:'))
        foozip.close()
        foo2zipSource = BytesIO(zipdata.getvalue())

        # Load the updated foo sources... next to the original
        srchash2 = self.systems['One'].loadActorSource(foo2zipSource)
        self.assertIsNotNone(srchash2)
        self.assertNotEqual(srchash, srchash2)

        self.systems['One'].unloadActorSource(srchash)
        self.systems['One'].tell(foo, ActorExitRequest())

        foo2 = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash2)
        self.assertRaises(InvalidActorSourceHash,
                          self.systems['One'].createActor,
                          'foo.FooActor', sourceHash=srchash)
        self.assertEqual('TOG: good one', self.systems['One'].ask(foo2, 'good one', 1))
        self.assertEqual('& MOO: great', self.systems['One'].ask(foo2, ('discard', 'great'), 1))

        self.assertEqual('COW: Moooo on 1', self.systems['One'].ask(foo2, 1, 1))

    def test06_sourceAuthorityCanRegister(self):
        self.startSystems(210)
        auth = self.systems['One'].createActor(rot13Authority)
        enabled = self.systems['One'].ask(auth, 'Enable', 1)
        self.assertEqual(enabled, 'Enabled')

    def test07_sourceAuthorityRejectsInvalidSource(self):
        self.startSystems(220)
        auth = self.systems['One'].createActor(rot13Authority)
        enabled = self.systems['One'].ask(auth, 'Enable', 1)
        self.assertEqual(enabled, 'Enabled')
        srchash = self._loadFooSource()
        self.assertIsNotNone(srchash)
        time.sleep(0.25)  # allow time for load to consult Source Authority
        self.assertRaises(InvalidActorSourceHash,
                          self.systems['One'].createActor,
                          'foo.FooActor', sourceHash = srchash)

    def test07_sourceAuthorityAcceptsValidSource(self):
        self.startSystems(230)
        auth = self.systems['One'].createActor(rot13Authority)
        enabled = self.systems['One'].ask(auth, 'Enable', 1)
        self.assertEqual(enabled, 'Enabled')
        srchash = self.systems['One'].loadActorSource(self.foozipEncFile)
        self.assertIsNotNone(srchash)
        time.sleep(0.25)  # allow time for load to consult Source Authority
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('And MOO: great', self.systems['One'].ask(foo, ('discard', 'great')))

    def test07_sourceAuthorityAcceptsValidSourceAfterBadSource(self):
        self.startSystems(240)
        auth = self.systems['One'].createActor(rot13Authority)
        enabled = self.systems['One'].ask(auth, 'Enable', 1)
        self.assertEqual(enabled, 'Enabled')
        failhash = self.systems['One'].loadActorSource(self.dogzipFname)
        srchash = self.systems['One'].loadActorSource(self.foozipEncFile)
        self.assertIsNotNone(srchash)
        time.sleep(0.25)  # allow time for load to consult Source Authority
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('And MOO: great', self.systems['One'].ask(foo, ('discard', 'great')))
        self.assertRaises(InvalidActorSourceHash,
                          self.systems['One'].createActor,
                          'dog.DogActor', sourceHash = failhash)

    def test07_sourceAuthorityAcceptsMultipleValidSources(self):
        self.startSystems(250)
        auth = self.systems['One'].createActor(rot13Authority)
        enabled = self.systems['One'].ask(auth, 'Enable', 1)
        self.assertEqual(enabled, 'Enabled')
        srchash = self.systems['One'].loadActorSource(self.foozipEncFile)
        self.assertIsNotNone(srchash)
        srchash2 = self.systems['One'].loadActorSource(self.dogzipEncFile)
        self.assertIsNotNone(srchash2)
        time.sleep(0.25)  # allow time for loads to consult Source Authority
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        dog = self.systems['One'].createActor('dog.DogActor', sourceHash=srchash2)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('And MOO: great', self.systems['One'].ask(foo, ('discard', 'great')))
        self.assertEqual('Woof! bark', self.systems['One'].ask(dog, 'bark', 1))

    def test07_multipleValidSourcesCanCommunicate(self):
        self.startSystems(260)
        auth = self.systems['One'].createActor(rot13Authority)
        enabled = self.systems['One'].ask(auth, 'Enable', 1)
        self.assertEqual(enabled, 'Enabled')
        srchash = self.systems['One'].loadActorSource(self.foozipEncFile)
        self.assertIsNotNone(srchash)
        srchash2 = self.systems['One'].loadActorSource(self.dogzipEncFile)
        self.assertIsNotNone(srchash2)
        time.sleep(0.25)  # allow time for loads to consult Source Authority
        cow = self.systems['One'].createActor('barn.cow.moo.MooActor', sourceHash=srchash)
        dog = self.systems['One'].createActor('dog.DogActor', sourceHash=srchash2)
        self.assertEqual('Moo: good one', self.systems['One'].ask(cow, 'good one', 1))
        self.assertEqual('Woof! good boy', self.systems['One'].ask(dog, 'good boy', 1))
        self.assertEqual('And MOO: Ruff Ruff: hungry',
                         self.systems['One'].ask(dog, ('hungry', cow), 1))

    def test07_sourceAuthorityAcceptsValidSourceResultIsCorrupted(self):
        self.startSystems(270)
        auth = self.systems['One'].createActor(rot13CorruptAuthority)
        enabled = self.systems['One'].ask(auth, 'Enable', 1)
        self.assertEqual(enabled, 'Enabled')
        srchash = self.systems['One'].loadActorSource(self.foozipEncFile)
        self.assertIsNotNone(srchash)
        time.sleep(0.25)  # allow time for load to consult Source Authority
        try:
            badfoo = self.systems['One'].createActor('foo.FooActor', sourceHash = srchash)
            self.assertFalse('Should not get here!')
        except (InvalidActorSourceHash, ImportError):
            self.assertTrue(True)  # Valid exceptions for a corrupt source
        except Exception as ex:
            self.assertFalse('Invalid exception thrown: %s'%str(ex))

    def test07_sourceAuthorityExceptions(self):
        self.startSystems(280)
        auth = self.systems['One'].createActor(rot13FailAuthority)
        enabled = self.systems['One'].ask(auth, 'Enable', 1)
        self.assertEqual(enabled, 'Enabled')
        srchash = self.systems['One'].loadActorSource(self.foozipEncFile)
        self.assertIsNotNone(srchash)
        time.sleep(0.25)  # allow time for load to consult Source Authority
        self.assertRaises(InvalidActorSourceHash,
                          self.systems['One'].createActor,
                          'foo.FooActor', sourceHash = srchash)

class rot13Authority(Actor):
    def receiveMessage(self, msg, sender):
        if msg == 'Enable':
            self.registerSourceAuthority()
            self.send(sender, 'Enabled')
        elif isinstance(msg, ValidateSource):
            clear = _decryptROT13(msg.sourceData)
            if clear:
                self.send(sender, ValidatedSource(msg.sourceHash, clear))

class rot13CorruptAuthority(Actor):
    def receiveMessage(self, msg, sender):
        if msg == 'Enable':
            self.registerSourceAuthority()
            self.send(sender, 'Enabled')
        elif isinstance(msg, ValidateSource):
            clear = _decryptROT13(msg.sourceData)
            if clear:
                corruption = 'corrupted' if isinstance(clear, str) else b'corrupted'
                for x in range(5, len(clear), len(corruption) + 100):
                    clear = clear[:x] + corruption + clear[x+len(corruption):]
                self.send(sender, ValidatedSource(msg.sourceHash, clear))

class rot13FailAuthority(Actor):
    def receiveMessage(self, msg, sender):
        if msg == 'Enable':
            self.registerSourceAuthority()
            self.send(sender, 'Enabled')
        elif isinstance(msg, ValidateSource):
            if msg.sourceData[:8] == 'ROT13___':
                raise ValueError('Oh no, I must go')


class TestMultiProcTCPSystem(TestASimpleSystem):
    testbase='MultiprocTCP'
    scope='func'
    actorSystemBase = 'multiprocTCPBase'
    portBase = 2


class TestMultiProcUDPSystem(TestASimpleSystem):
    testbase='MultiprocUDP'
    scope='func'
    actorSystemBase = 'multiprocUDPBase'
    portBase = 4

class TestMultiProcQueueSystem(TestASimpleSystem):
    testbase='MultiprocQueue'
    scope='func'
    actorSystemBase = 'multiprocQueueBase'
    portBase = 8



class TestMultipleMultiProcTCPSystem(ActorSystemTestCase):
    testbase='MultiprocTCP'
    scope='func'
    actorSystemBase = 'multiprocTCPBase'
    portBase = 1000

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

        self.foozipFname = os.path.join(self.tmpdir, 'foosrc.zip')
        foozip = zipfile.ZipFile(self.foozipFname, 'w')
        foozip.writestr('__init__.py', '')
        foozip.writestr('frog.py', frogSource)
        foozip.writestr('toad.py', toadSource)
        foozip.writestr('foo.py', fooSource)
        foozip.writestr('barn/__init__.py', '')
        foozip.writestr('barn/cow/__init__.py', '')
        foozip.writestr('barn/cow/moo.py', mooSource)
        foozip.close()

        self.foozipEncFile = _encryptROT13Zipfile(self.foozipFname)

        self.dogzipFname = os.path.join(self.tmpdir, 'dogsrc.zip')
        dogzip = zipfile.ZipFile(self.dogzipFname, 'w')
        dogzip.writestr('dog.py', dogSource)
        dogzip.close()

        self.dogzipEncFile = _encryptROT13Zipfile(self.dogzipFname)
        self.systems = {}

    def startSystems(self, portOffset):
        # Only define base capabilities, not extended capabilities
        self.capabilities = { 'One': { 'Admin Port': 30000 + portOffset + self.portBase, },
                              'Two': { 'Admin Port': 30001 + portOffset + self.portBase,
                                       'Convention Address.IPv4': ('', 30000 + portOffset + self.portBase), },
                          }
        for each in ['One', 'Two']:  # 'One' must be first
            self.systems[each] = ActorSystem(self.actorSystemBase, self.capabilities[each],
                                             logDefs = simpleActorTestLogging(),
                                             transientUnique = True)
        time.sleep(0.5)  # Wait for Actor Systems to start

    def tearDown(self):
        for each in self.systems:
            self.systems[each].shutdown()
        import shutil
        if os.path.exists(self.tmpdir): shutil.rmtree(self.tmpdir)

    def test00_systemsRunnable(self):
        self.startSystems(300)
        pass

    def test09_properErrorIfActorCapabilitiesNotSatisfied(self):
        self.startSystems(310)
        # Specify a Source Authority and load the foo sources
        auth = self.systems['One'].createActor(rot13Authority)
        enabled = self.systems['One'].ask(auth, 'Enable', 1)
        self.assertEqual(enabled, 'Enabled')
        srchash = self.systems['One'].loadActorSource(self.foozipEncFile)
        self.assertIsNotNone(srchash)
        # Establish capabilities that allow Foo and Moo actors (in different systems)
        time.sleep(0.25)  # allow time for load to consult Source Authority
        self.assertRaises(NoCompatibleSystemForActor,
                          self.systems['One'].createActor,
                          'foo.FooActor', sourceHash=srchash)

    def test08_multiSystemSharesLoadedSourcesByDefault(self):
        self.startSystems(320)
        # Specify a Source Authority and load the foo sources
        auth = self.systems['One'].createActor(rot13Authority)
        enabled = self.systems['One'].ask(auth, 'Enable', 1)
        self.assertEqual(enabled, 'Enabled')
        srchash = self.systems['One'].loadActorSource(self.foozipEncFile)
        self.assertIsNotNone(srchash)
        self.systems['One'].updateCapability('Foo Allowed', True)
        self.systems['Two'].updateCapability('Cows Allowed', True)
        # Establish capabilities that allow Foo and Moo actors (in different systems)
        time.sleep(0.25)  # allow time for load to consult Source Authority

        # Verify that FooActor can be created (locally) and it can
        # create MooActor remotely, where the remote system will
        # obtain the sources from the local system as needed.
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('And MOO: great',
                         self.systems['One'].ask(foo, ('discard', 'great'), 1))

    def test08_loadableInRemoteSystemMatchingCapabilities(self):
        self.startSystems(330)
        # Specify a Source Authority and load the foo sources
        auth = self.systems['One'].createActor(rot13Authority)
        enabled = self.systems['One'].ask(auth, 'Enable', 1)
        self.assertEqual(enabled, 'Enabled')
        srchash = self.systems['One'].loadActorSource(self.foozipEncFile)
        self.assertIsNotNone(srchash)
        self.systems['Two'].updateCapability('Foo Allowed', True)
        self.systems['One'].updateCapability('Cows Allowed', True)
        # Establish capabilities that allow Foo and Moo actors (in different systems)
        time.sleep(0.85)  # allow time for load to consult Source Authority

        # Verify that FooActor can be created (locally) and it can
        # create MooActor remotely, where the remote system will
        # obtain the sources from the local system as needed.
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('And MOO: great',
                         self.systems['One'].ask(foo, ('discard', 'great'), 1))

    def test08_multiSystemSharesLoadedSourcesIfExplicitlyAllowed(self):
        self.startSystems(340)
        # Specify a Source Authority and load the foo sources
        auth = self.systems['One'].createActor(rot13Authority)
        enabled = self.systems['One'].ask(auth, 'Enable', 1)
        self.assertEqual(enabled, 'Enabled')
        srchash = self.systems['One'].loadActorSource(self.foozipEncFile)
        self.assertIsNotNone(srchash)
        self.systems['One'].updateCapability('Foo Allowed', True)
        self.systems['Two'].updateCapability('Cows Allowed', True)
        self.systems['Two'].updateCapability('AllowRemoteActorSources', 'yes')
        time.sleep(0.25)  # Allow for Hysteresis delay of two updates from system Two
        # Establish capabilities that allow Foo and Moo actors (in different systems)
        time.sleep(0.25)  # allow time for load to consult Source Authority

        # Verify that FooActor can be created (locally) and it can
        # create MooActor remotely, where the remote system will
        # obtain the sources from the local system as needed.
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('And MOO: great',
                         self.systems['One'].ask(foo, ('discard', 'great'), 1))

    def test08_multiSystemLoadedSourcesNotSharedIfExplicitlyDisallowed(self):
        self.startSystems(350)
        # Specify a Source Authority and load the foo sources
        auth = self.systems['One'].createActor(rot13Authority)
        enabled = self.systems['One'].ask(auth, 'Enable', 1)
        self.assertEqual(enabled, 'Enabled')
        srchash = self.systems['One'].loadActorSource(self.foozipEncFile)
        self.assertIsNotNone(srchash)
        self.systems['One'].updateCapability('Foo Allowed', True)
        self.systems['Two'].updateCapability('Cows Allowed', True)
        self.systems['Two'].updateCapability('AllowRemoteActorSources', 'no')
        # Establish capabilities that allow Foo and Moo actors (in different systems)
        time.sleep(0.25)  # allow time for load to consult Source Authority

        # Verify that FooActor can be created (locally) and it can
        # create MooActor remotely, where the remote system will
        # obtain the sources from the local system as needed.
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('FAILED (poisonous)',
                         self.systems['One'].ask(foo, ('discard', 'great'), 1))

    def test08_multiSystemLoadedSourcesNotSharedIfSharingUnrecognized(self):
        self.startSystems(360)
        # Specify a Source Authority and load the foo sources
        auth = self.systems['One'].createActor(rot13Authority)
        enabled = self.systems['One'].ask(auth, 'Enable', 1)
        self.assertEqual(enabled, 'Enabled')
        srchash = self.systems['One'].loadActorSource(self.foozipEncFile)
        self.assertIsNotNone(srchash)
        self.systems['One'].updateCapability('Foo Allowed', True)
        self.systems['Two'].updateCapability('Cows Allowed', True)
        self.systems['Two'].updateCapability('AllowRemoteActorSources', 'whatever!')
        # Establish capabilities that allow Foo and Moo actors (in different systems)
        time.sleep(0.25)  # allow time for load to consult Source Authority

        # Verify that FooActor can be created (locally) and it can
        # create MooActor remotely, where the remote system will
        # obtain the sources from the local system as needed.
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('FAILED (poisonous)',
                         self.systems['One'].ask(foo, ('discard', 'great'), 1))

    def test08_loadableInRemoteSystemOnlyIfSourceComesFromConventionLeader(self):
        self.startSystems(370)
        auth = self.systems['Two'].createActor(rot13Authority)
        enabled = self.systems['Two'].ask(auth, 'Enable', 1)
        self.assertEqual(enabled, 'Enabled')
        srchash = self.systems['Two'].loadActorSource(self.foozipEncFile)
        self.assertIsNotNone(srchash)
        time.sleep(0.1)

        self.systems['Two'].updateCapability('Foo Allowed', True)
        self.systems['One'].updateCapability('Cows Allowed', True)
        self.systems['One'].updateCapability('AllowRemoteActorSources', 'LeaderOnly')
        self.systems['Two'].updateCapability('AllowRemoteActorSources', 'LeaderOnly')
        # Establish capabilities that allow Foo and Moo actors (in different systems)
        time.sleep(0.25)  # allow time for load to consult Source Authority

        self.assertRaises(InvalidActorSourceHash,
                          self.systems['One'].createActor,
                          'foo.FooActor', sourceHash=srchash)

        self.systems['One'].updateCapability('AllowRemoteActorSources', 'yes')
        time.sleep(0.08)
        foo = self.systems['Two'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('And MOO: great',
                         self.systems['One'].ask(foo, ('discard', 'great'), 1))

    def test09_loadableInRemoteSystemUnloadedOnPrimaryUnload(self):
        self.startSystems(380)
        auth = self.systems['One'].createActor(rot13Authority)
        enabled = self.systems['One'].ask(auth, 'Enable', 1)
        self.assertEqual(enabled, 'Enabled')
        srchash = self.systems['One'].loadActorSource(self.foozipEncFile)
        self.assertIsNotNone(srchash)
        time.sleep(0.1)

        self.systems['Two'].updateCapability('Foo Allowed', True)
        self.systems['One'].updateCapability('Cows Allowed', True)
        time.sleep(0.2) # Allow updates to propagate

        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('And MOO: great',
                         self.systems['One'].ask(foo, ('discard', 'great'), 1))
        foo2 = self.systems['Two'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: another good one', self.systems['One'].ask(foo2, 'another good one', 1))

        # Now unload source and kill actors; they cannot be recreated on either system.
        self.systems['One'].tell(foo, ActorExitRequest())
        self.systems['One'].tell(foo2, ActorExitRequest())
        self.systems['One'].unloadActorSource(srchash)

        self.assertRaises(InvalidActorSourceHash,
                          self.systems['One'].createActor, 'foo.FooActor', sourceHash=srchash)
        self.assertRaises(InvalidActorSourceHash,
                          self.systems['Two'].createActor, 'foo.FooActor', sourceHash=srchash)


    def test09_loadableInRemoteSystemUnloadedOnMemberUnload(self):
        self.startSystems(390)
        auth = self.systems['One'].createActor(rot13Authority)
        enabled = self.systems['One'].ask(auth, 'Enable', 1)
        self.assertEqual(enabled, 'Enabled')
        srchash = self.systems['One'].loadActorSource(self.foozipEncFile)
        self.assertIsNotNone(srchash)
        time.sleep(0.1)

        self.systems['Two'].updateCapability('Foo Allowed', True)
        self.systems['One'].updateCapability('Cows Allowed', True)
        time.sleep(0.8) # Allow updates to propagate

        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('And MOO: great',
                         self.systems['One'].ask(foo, ('discard', 'great'), 1))
        foo2 = self.systems['Two'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: another good one', self.systems['One'].ask(foo2, 'another good one', 1))

        # Now unload source and kill actors; they cannot be recreated on either system.
        self.systems['One'].tell(foo, ActorExitRequest())
        self.systems['One'].tell(foo2, ActorExitRequest())
        self.systems['Two'].unloadActorSource(srchash)

        self.assertRaises(InvalidActorSourceHash,
                          self.systems['One'].createActor, 'foo.FooActor', sourceHash=srchash)
        self.assertRaises(InvalidActorSourceHash,
                          self.systems['Two'].createActor, 'foo.FooActor', sourceHash=srchash)


    def test10_multiSystemStartSubActorByClassReferenceLocally(self):
        self.startSystems(400)
        auth = self.systems['One'].createActor(rot13Authority)
        enabled = self.systems['One'].ask(auth, 'Enable', 1)
        self.assertEqual(enabled, 'Enabled')
        srchash = self.systems['One'].loadActorSource(self.foozipEncFile)
        self.assertIsNotNone(srchash)
        self.systems['One'].updateCapability('Foo Allowed', True)
        self.systems['Two'].updateCapability('Cows Allowed', True)
        # Establish capabilities that allow Foo and Moo actors (in different systems)
        time.sleep(0.2)  # allow time for load to consult Source Authority

        # Verify that FooActor can be created (locally) and it can
        # create MooActor remotely, where the remote system will
        # obtain the sources from the local system as needed.
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('In a WORLD: Where Pigs Fly',
                         self.systems['One'].ask(foo, [{'Foo Allowed': True},
                                                       'Where Pigs Fly'],
                                                 1))

    def test10_multiSystemStartSubActorByClassReferenceRemotely(self):
        self.startSystems(410)
        auth = self.systems['One'].createActor(rot13Authority)
        enabled = self.systems['One'].ask(auth, 'Enable', 1)
        self.assertEqual(enabled, 'Enabled')
        srchash = self.systems['One'].loadActorSource(self.foozipEncFile)
        self.assertIsNotNone(srchash)
        self.systems['One'].updateCapability('Foo Allowed', True)
        self.systems['Two'].updateCapability('Cows Allowed', True)
        # Establish capabilities that allow Foo and Moo actors (in different systems)
        time.sleep(0.2)  # allow time for load to consult Source Authority

        # Verify that FooActor can be created (locally) and it can
        # create MooActor remotely, where the remote system will
        # obtain the sources from the local system as needed.
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('In a WORLD: Where Pigs Fly',
                         self.systems['One'].ask(foo, [{'Cows Allowed': True},
                                                       'Where Pigs Fly'],
                                                 1))

    def test10_multiSystemStartSubActorCannotStart(self):
        self.startSystems(420)
        auth = self.systems['One'].createActor(rot13Authority)
        enabled = self.systems['One'].ask(auth, 'Enable', 1)
        self.assertEqual(enabled, 'Enabled')
        srchash = self.systems['One'].loadActorSource(self.foozipEncFile)
        self.assertIsNotNone(srchash)
        self.systems['One'].updateCapability('Foo Allowed', True)
        self.systems['Two'].updateCapability('Cows Allowed', True)
        # Establish capabilities that allow Foo and Moo actors (in different systems)
        time.sleep(0.2)  # allow time for load to consult Source Authority

        # Verify that FooActor can be created (locally) and it can
        # create MooActor remotely, where the remote system will
        # obtain the sources from the local system as needed.
        foo = self.systems['One'].createActor('foo.FooActor', sourceHash=srchash)
        self.assertEqual('GOT: good one', self.systems['One'].ask(foo, 'good one', 1))
        self.assertEqual('FAILED (poisonous)',
                         self.systems['One'].ask(foo, [{'Elephants Allowed': True},
                                                       'Where Pigs Fly'],
                                                 1))


class TestMultipleMultiProcUDPSystem(TestMultipleMultiProcTCPSystem):
    testbase='MultiprocUDP'
    scope='func'
    actorSystemBase = 'multiprocUDPBase'
    portBase = 1003
