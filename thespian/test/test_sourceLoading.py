from thespian.test import *
import time
from thespian.actors import *
import zipfile
import tempfile
import os, sys
import copy
import shutil
from datetime import timedelta
from pytest import raises, mark
from thespian.system.utilis import thesplog

sourceauthority_reg_wait = lambda: inTestDelay(timedelta(milliseconds=100))
sourceload_wait = lambda: inTestDelay(timedelta(milliseconds=300))
sourceunload_wait = lambda: inTestDelay(timedelta(milliseconds=200))
ask_wait = timedelta(seconds=15)


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

fishSource = '''
from thespian.actors import ActorTypeDispatcher, requireCapability

@requireCapability('Water')
class FishActor(ActorTypeDispatcher):
    def receiveMsg_str(self, strmsg, sender):
        self.send(sender, 'Bubble(%s)'%strmsg)
'''

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
from thespian.actors import ActorTypeDispatcher, requireCapability
from datetime import timedelta

@requireCapability('Dogs Allowed')
class DogActor(ActorTypeDispatcher):
    def receiveMsg_str(self, msg, sender):
        self.send(sender, 'Woof! '+str(msg))
    def receiveMsg_tuple(self, msg, sender):
        self.send(msg[1], ('Ruff Ruff: ' + str(msg[0]), sender))
    def receiveMsg_list(self, msg, sender):
        newHash = self.loadActorSource(msg[0])
        # Need to wait for source to load, but waiting with a sleep
        # may prevent the load request from processing, so use wakeupAfter
        if not hasattr(self, 'tgtsends'):
            self.tgtsends = []
        self.tgtsends.append( (msg, sender, newHash) )
        self.wakeupAfter(timedelta(milliseconds=15))
    def receiveMsg_WakeupMessage(self, wakeupmsg, wakeupsender):
        pending = self.tgtsends
        self.tgtsends = []
        for msg, sender, newHash in pending:
            newA = self.createActor(msg[1], sourceHash = newHash)
            self.send(newA, ('Bark! ' + msg[2], sender))
            self.unloadActorSource(newHash)
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

# roo exercises deep imports, indirect through kanga
rooSource = '''
import thespian.actors
#import pooh
import pooh.corner.kanga
#from pooh.kanga import *
class RooActor(thespian.actors.Actor):
    def receiveMessage(self, msg, sender):
        if type(msg) == type(""):
            self.send(sender, pooh.corner.kanga.kanga(msg))
'''

kangaSource = '''
import thespian.actors
import gorse.bush.eeyore
def kanga(msgstr): return msgstr + ' ' + gorse.bush.eeyore.says()
'''

eeyoreSource = '''
def says(): return 'whatever'
'''

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

ouroborosSource = '''
import sys
import worm.jormungandr
import os

def ouroboros(): return 'endless'

from thespian.actors import ActorTypeDispatcher

class Serpent(ActorTypeDispatcher):
    def receiveMsg_str(self, msg, sender):
        self.send(sender, '<<' + ouroboros() + msg + worm.jormungandr.jormungandr() + '>>')
'''

jormungandrSource = '''
print('j1')
import string
print('j2')
import ouroboros
print('j3')
import logging
print('j4')

def jormungandr(): return 'dragon'
'''


lizardSource = '''
# Direct attempts to access the import machinery, a la sqlalchemy

"This is a module docstring"


from __future__ import with_statement

from __future__ import print_function
import sys
py3k = sys.version_info >= (3,0)
if py3k:
    import builtins
    builtins.__dict__['__import__'] = __builtins__['__import__']
    import_ = getattr(builtins, '__import__')
else:
    def import_(*args):
        if len(args) == 4:
            args = args[0:3] + ([str(arg) for arg in args[3]],)
        return __import__(*args)

#from rock.worm import wormy

earth = import_('rock.worm', globals(), locals(), ['wormy'])

from thespian.actors import *

class Lizard(Actor):
    def receiveMessage(self, message, sender):
        self.send(sender, earth.wormy(message))
'''

class BarActor(Actor):
    def receiveMessage(self, msg, sender):
        if type(msg) == type(""):
            self.send(sender, 'SAW: '+str(msg))


class SimpleSourceAuthority(ActorTypeDispatcher):
    def receiveMsg_str(self, msg, sender):
        self.registerSourceAuthority()
    def receiveMsg_ValidateSource(self, msg, sender):
        self.send(sender, ValidatedSource(msg.sourceHash, msg.sourceData))


class LoadWatcher(ActorTypeDispatcher):
    def receiveMsg_str(self, msg, sender):
        if msg == 'go':
            self.sources_yes = []
            self.sources_no = []
            self.notifyOnSourceAvailability(True)
        elif msg == 'stop':
            self.notifyOnSourceAvailability(False)
        elif msg == 'what is loaded?':
            self.send(sender, ', '.join(['%s=Yes'%H for H in self.sources_yes] +
                                        ['%s=No'%H for H in self.sources_no]))
    def receiveMsg_LoadedSource(self, loadmsg, sender):
        self.sources_no = list(filter(lambda h: h != loadmsg.sourceHash,
                                      self.sources_no))
        # Allow multiple "loads" to show multiple times
        self.sources_yes.append(loadmsg.sourceHash)
        # n.b. the print validates the presence but not the value of
        # loadmsg.sourceInfo
        print('Notification of load of %s (%s)' % (loadmsg.sourceHash,
                                                   loadmsg.sourceInfo))
    def receiveMsg_UnloadedSource(self, unloadmsg, sender):
        self.sources_yes = list(filter(lambda h: h != unloadmsg.sourceHash,
                                      self.sources_yes))
        # Allow multiple unloads to show multiple times
        self.sources_no.append(unloadmsg.sourceHash)
        # n.b. the print validates the presence but not the value of
        # loadmsg.sourceInfo
        print('Notification of unload of %s (%s)' % (unloadmsg.sourceHash,
                                                     unloadmsg.sourceInfo))

@pytest.fixture(scope='class')
def source_zips(request):
    tmpdir = tempfile.mkdtemp()

    # n.b. if this gets too long, the UDPTransport will be unable to
    # transfer the source zip files to other Actor Systems (UDP packet
    # size is limited and it does not have the ability to split and
    # reconstruct packages natively.
    foozipFname = os.path.join(tmpdir, 'foosrc.zip')
    foozip = zipfile.ZipFile(foozipFname, 'w')
    foozip.writestr('__init__.py', '')
    foozip.writestr('foo.py', fooSource)
    foozip.writestr('frog.py', frogSource)
    foozip.writestr('toad.py', toadSource)
    foozip.writestr('lizard.py', lizardSource)
    foozip.writestr('rock/__init__.py', '')
    foozip.writestr('rock/worm.py', 'def wormy(v): return "Slimy " + v')
    foozip.writestr('barn/__init__.py', barnInitSource)
    foozip.writestr('barn/pig.py', pigSource)
    foozip.writestr('barn/chicken.py', chickenSource)
    foozip.writestr('barn/rooster.py', roosterSource)
    foozip.writestr('barn/goose.py', gooseSource)
    foozip.writestr('barn/sow.py', sowSource)
    foozip.writestr('fish.py', fishSource)
    foozip.writestr('pooh/__init__.py', '')
    foozip.writestr('pooh/corner/__init__.py', '')
    foozip.writestr('gorse/__init__.py', '')
    foozip.writestr('gorse/bush/__init__.py', '')
    foozip.writestr('gorse/bush/eeyore.py', eeyoreSource)
    foozip.writestr('pooh/corner/kanga.py', kangaSource)
    foozip.writestr('barn/piglet.py', pigletSource)
    foozip.writestr('ouroboros.py', ouroborosSource)
    foozip.writestr('worm/__init__.py', '')
    foozip.writestr('worm/jormungandr.py', jormungandrSource)
    foozip.writestr('barn/cow/__init__.py', '')
    foozip.writestr('barn/cow/moo.py', mooSource)
    foozip.writestr('roo.py', rooSource)
    foozip.close()

    foozipEncFile = _encryptROT13Zipfile(foozipFname)

    dogzipFname = os.path.join(tmpdir, 'dogsrc.zip')
    dogzip = zipfile.ZipFile(dogzipFname, 'w')
    dogzip.writestr('dog.py', dogSource)
    dogzip.close()

    dogzipEncFile = _encryptROT13Zipfile(dogzipFname)

    request.addfinalizer(lambda d=tmpdir: os.path.exists(d) and shutil.rmtree(d))

    return tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile



class TestUnitRoundTripROT13(object):

    def test_simple_rot13_enc_dec(self, source_zips):
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        simplezipFname = os.path.join(tmpdir, 'simple.zip')
        zf = open(simplezipFname, 'wb')
        zf.write(b'abcdABCD1234')
        zf.close()
        encfname = _encryptROT13Zipfile(simplezipFname)
        encdata = open(encfname, 'rb').read()
        decdata = _decryptROT13(encdata)
        assert decdata == b'abcdABCD1234'

    def test_rot13_enc_dec(self, source_zips):
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        encf = open(foozipEncFile, 'rb')
        encdata = encf.read()
        encf.close()
        foozipDecoded = _decryptROT13(encdata)
        from io import BytesIO
        from zipfile import ZipFile
        foozip = ZipFile(BytesIO(foozipDecoded))

        names = foozip.namelist()
        assert names[0] == '__init__.py'
        assert names[-2] == 'barn/cow/moo.py'
        assert names[-1] == 'roo.py'
        assert len(names) == 27


@pytest.fixture(scope='class')
def sys_path(request):
    origpath = copy.deepcopy(sys.path)
    def reset_path(): sys.path = origpath
    request.addfinalizer(reset_path)
    return True


# NOTE: this is not a "class Test..." because it should not run as
# part of the normal Thespian testing.  Running this test pollutes the
# local module namespace and causes other tests to fail.  This test is
# primarily used to establish that the ZipFile created is useable via
# normal import techniques; any change to zipfile contents should be
# first validated explicitly with the tests in this class before
# attempting to have Thespian importing conform to the use of that
# zipfile.
class BaselineDirectZipfile(object):
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
    # this, there is a class-level fixture for zipfile management and
    # a class-level fixture for path management.

    def testFooString(self, source_zips, sys_path):
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        sys.path.insert(0, foozipFname)
        # First try to import foo itself from the zipfile
        import foo
        f = foo.FooActor()
        # calling the FooActor with a string causes some additional
        # non-module-level imports to occur.  If it makes it past the
        # imports, it will try to send back a response which will fail
        # since we are using the invalid address of "sender".
        raises(InvalidActorAddress, f.receiveMessage, "hi", "sender")

    def testFooInteger(self, source_zips, sys_path):
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        sys.path.insert(0, foozipFname)
        # First try to import foo itself from the zipfile
        import foo
        f = foo.FooActor()
        # calling the FooActor with an integer causes some additional
        # non-module-level imports to occur that are *different* than
        # the ones that occur when it gets passed a string.  If it
        # makes it past the imports, it will try to send back a
        # response which will fail since we are using the invalid
        # address of "sender".
        raises(InvalidActorAddress, f.receiveMessage, 5, "sender")

    def testPig(self, source_zips, sys_path):
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        sys.path.insert(0, foozipFname)
        # First try to import barn top level and pig from the zipfile
        import barn.pig
        f = barn.pig.PigActor()
        # calling the PigActor with a string causes some additional
        # non-module-level imports to occur.
        raises(InvalidActorAddress, f.receiveMessage, "what", "sender")

    def testSow(self, source_zips, sys_path):
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        sys.path.insert(0, foozipFname)
        # First try to import barn top level and sow from the zipfile
        import barn.sow
        f = barn.sow.SowActor()
        # calling the SowActor with a string causes some additional
        # non-module-level imports to occur.
        raises(InvalidActorAddress, f.receiveMessage, "what", "sender")

    @mark.skipif("sys.version_info >= (3,0)",
                 reason="Python 2 import syntax only")
    def testPiglet(self, source_zips, sys_path):
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        sys.path.insert(0, foozipFname)
        # First try to import barn top level and piglet from the zipfile
        import barn.piglet
        f = barn.piglet.PigletActor()
        # calling the PigletActor with a string causes some additional
        # non-module-level imports to occur.
        raises(InvalidActorAddress, f.receiveMessage, "what", "sender")

    def testRoo(self, source_zips, sys_path):
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        sys.path.insert(0, foozipFname)
        import roo
        f = roo.RooActor()
        # call with a string to make sure deep import references resolve
        raises(InvalidActorAddress, f.receiveMessage, "roo", "sender")

    def testOuroboros(self, source_zips, sys_path):
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        sys.path.insert(0, foozipFname)
        import ouroboros
        assert ouroboros.ouroboros() == 'endless'
        assert ouroboros.worm.jormungandr.jormungandr() == 'dragon'


class TestFuncLoadSource(object):

    def _registerSA(self, asys):
        asys.tell(asys.createActor(SimpleSourceAuthority), 'go')
        sourceauthority_reg_wait() # wait for source authorities to register

    def _registerLoadNotifications(self, asys):
        watcher = asys.createActor(LoadWatcher)
        asys.tell(watcher, 'go')
        sourceauthority_reg_wait() # wait for start and registration of load watcher
        return watcher

    def test00_systemsRunnable(self, asys):
        pass

    def test01_verifyFooActorNotAvailableByName(self, asys):
        self._registerSA(asys)
        raises(ImportError, asys.createActor, 'foo.FooActor')
        bar = asys.createActor('thespian.test.test_sourceLoading.BarActor')
        r = asys.ask(bar, 'hello', ask_wait)
        assert 'SAW: hello' == r

    def test01_verifyFooActorNotAvailableWithBogusSourceHash(self, asys):
        self._registerSA(asys)
        raises(InvalidActorSourceHash,
               asys.createActor,
               'foo.FooActor', sourceHash = 'this is bogus')

    def test01_verifyloadSourceHandlesBadFilename(self, asys):
        self._registerSA(asys)
        raises(IOError, asys.loadActorSource, 'bad file name here')


    def _loadFooSource(self, asys, source_zips):
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        srchash = asys.loadActorSource(foozipFname)
        sourceload_wait() # allow time for validation of source by source authority
        assert srchash is not None
        return srchash

    def test02_verifyMainActorAvailableWhenLoaded(self, asys, source_zips):
        thesplog('tt1 %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash = self._loadFooSource(asys, source_zips)
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r

    def _verifyHashNotification(self, asys, loadnotify, srchash, loaded=True):
        srchash_loaded = '%s=%s' % (srchash, 'Yes' if loaded else 'No')
        for tries in range(5):
            r = asys.ask(loadnotify, 'what is loaded?', ask_wait)
            print(srchash,'loaded:',r)
            if r and srchash_loaded in r:
                break
            time.sleep(0.005)
        else:
            assert 'No source load notification of %s' % srchash == r
        x = r.find(srchash_loaded)
        # Verify only one load notification was received
        assert -1 == r[x+len(srchash_loaded):].find(srchash_loaded)

    def test02_verifyNotificationNotAvailableOnActorSystemItself(self, asys):
        assert not hasattr(asys, 'notifyOnSourceAvailability')

    def test02_verifyNotificationAndMainActorCreateWhenLoaded(self, asys, source_zips):
        thesplog('tt1 %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        loadnotify = self._registerLoadNotifications(asys)
        srchash = self._loadFooSource(asys, source_zips)
        self._verifyHashNotification(asys, loadnotify, srchash)
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r

    def test02_verifyNotificationMultipleAndMainActorCreateWhenLoaded(self, asys, source_zips):
        thesplog('tt1 %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        loadnotify = self._registerLoadNotifications(asys)
        srchash = self._loadFooSource(asys, source_zips)
        self._verifyHashNotification(asys, loadnotify, srchash)
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        loadnotify2 = self._registerLoadNotifications(asys)
        self._verifyHashNotification(asys, loadnotify2, srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r

    def test02_verifyNotificationOfLoadedOnRegistration(self, asys, source_zips):
        thesplog('tt1 %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash = self._loadFooSource(asys, source_zips)
        loadnotify = self._registerLoadNotifications(asys)
        self._verifyHashNotification(asys, loadnotify, srchash)
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r

    def test02_verifyNoSourceAuthorityIgnoresLoad(self, asys, source_zips):
        thesplog('tt2 %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        srchash = self._loadFooSource(asys, source_zips)
        raises(InvalidActorSourceHash, asys.createActor, 'foo.FooActor', sourceHash=srchash)

    def test02_verifyNotificationNotSentWithNoSourceAuthority(self, asys, source_zips):
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        loadnotify = self._registerLoadNotifications(asys)
        srchash = self._loadFooSource(asys, source_zips)
        r = asys.ask(loadnotify, 'what is loaded?', ask_wait)
        assert r == ''


    def test02_verifyNotificationStopDoesNotNotify(self, asys, source_zips):
        thesplog('tt1 %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        loadnotify = self._registerLoadNotifications(asys)
        asys.tell(loadnotify, 'stop')
        sourceload_wait()
        srchash = self._loadFooSource(asys, source_zips)
        sourceload_wait()
        srchash_loaded = '%s=Yes' % srchash
        for tries in range(3):
            r = asys.ask(loadnotify, 'what is loaded?', ask_wait)
            print(srchash,'loaded:',r)
            assert srchash_loaded not in r
            time.sleep(0.005)
        # No notification, but srchash still useable
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r

    def test02_verifyNotificationKillDoesNotBreakNotify(self, asys, source_zips):
        thesplog('tt1 %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        loadnotify = self._registerLoadNotifications(asys)
        asys.tell(loadnotify, ActorExitRequest())
        sourceload_wait()
        srchash = self._loadFooSource(asys, source_zips)
        sourceload_wait()
        r = asys.ask(loadnotify, 'what is loaded?', timedelta(milliseconds=300))
        assert r is None
        # No notification, but srchash still useable
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r

    def test02_verifyNotificationOfMultipleKillOneDoesNotBreakNotify(self, asys, source_zips):
        thesplog('tt1 %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        loadnotify1 = self._registerLoadNotifications(asys)
        loadnotify2 = self._registerLoadNotifications(asys)
        asys.tell(loadnotify1, ActorExitRequest())
        sourceload_wait()
        srchash = self._loadFooSource(asys, source_zips)
        sourceload_wait()
        r = asys.ask(loadnotify1, 'what is loaded?', timedelta(milliseconds=300))
        assert r is None
        self._verifyHashNotification(asys, loadnotify2, srchash)
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r

    def test02_verifySubActorAvailableWhenLoaded(self, asys, source_zips):
        thesplog('tt3 %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash = self._loadFooSource(asys, source_zips)
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, ('discard', 'great'), ask_wait)
        assert 'And MOO: great' == r

    def test02_verifySubModuleAvailableWhenLoaded(self, asys, source_zips):
        thesplog('tt4 %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash = self._loadFooSource(asys, source_zips)
        cow = asys.createActor('barn.cow.moo.MooActor', sourceHash=srchash)
        r = asys.ask(cow, 'got milk', ask_wait)
        assert 'Moo: got milk' == r

    def test02_verifyAllAbsoluteImportPossibilities(self, asys, source_zips):
        thesplog('tt5 %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash = self._loadFooSource(asys, source_zips)
        pig = asys.createActor('barn.pig.PigActor', sourceHash=srchash)
        r = asys.ask(pig, 'ready?', ask_wait)
        assert 'Oink Cluck Honk ready? Moooo' == r

    def test02_verifyBuiltinImport(self, asys, source_zips):
        actor_system_unsupported(asys, 'simpleSystemBase')
        thesplog('tt6 %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash = self._loadFooSource(asys, source_zips)
        sourceload_wait() # allow time for validation of source by source authority
        lizard = asys.createActor('lizard.Lizard', sourceHash=srchash)
        r = asys.ask(lizard, 'goo', ask_wait)
        assert 'Slimy goo' == r

    def test02_verifyAllRelativeImportPossibilities(self, asys, source_zips):
        thesplog('tt6 %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash = self._loadFooSource(asys, source_zips)
        sow = asys.createActor('barn.sow.SowActor', sourceHash=srchash)
        r = asys.ask(sow, 'ready?', ask_wait)
        assert 'Moooo Oink Cluck Honk ready?' == r

    @mark.skipif("sys.version_info >= (3,0)", reason="Python 2 version imports")
    def test02_verifyAllOLDSTYLERelativeImportPossibilities(self, asys, source_zips):
        thesplog('tt7 %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash = self._loadFooSource(asys, source_zips)
        piglet = asys.createActor('barn.piglet.PigletActor', sourceHash=srchash)
        r = asys.ask(piglet, 'ready?', ask_wait)
        assert 'Moooo Oink Cluck Honk ready?' == r

    def test02_verifyDeepImportReferences(self, asys, source_zips):
        thesplog('tt8 %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash = self._loadFooSource(asys, source_zips)
        roo = asys.createActor('roo.RooActor', sourceHash=srchash)
        r = asys.ask(roo, 'roo says', ask_wait)
        assert 'roo says whatever' == r

    def test02_verifyHashSourceAvailablePostLoadFromMembers(self, asys, source_zips):
        thesplog('tt9 %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash = self._loadFooSource(asys, source_zips)
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        # Cause a load-on-demand of a new module (in the loaded
        # sources) from the loaded sources themselves and ensure it's still available.
        r = asys.ask(foo, 1, ask_wait)
        assert 'COW: Moooo on 1' == r

    def test02_verifyHashSourceNotInGlobalNamespace(self, asys, source_zips):
        thesplog('tta %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash = self._loadFooSource(asys, source_zips)
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        # Verify loaded source is not accessible globally
        try:
            from bar.cow.moo import cow_says
            assert False  # should never get here
        except ImportError:
            assert True   # want this
        except Exception:
            assert False  # but not these


    # Note: Perform these after the successful load tests (test02_) to
    # ensure that the modules loaded by those tests are no longer
    # available in the namespace to cause these tests (test03_) to fail.
    def test03_verifyFooActorNotAvailableWithoutModuleQualifiersOrHash(self, asys, source_zips):
        thesplog('ttb %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash = self._loadFooSource(asys, source_zips)
        raises(InvalidActorSpecification, asys.createActor, 'FooActor')

    def test03_verifyFooActorNotAvailableWithoutCorrectHash(self, asys, source_zips):
        thesplog('ttc %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        # No sourceHash specified, so the module foo is searched for
        # in the standard search path.
        srchash = self._loadFooSource(asys, source_zips)
        raises(ImportError, asys.createActor, 'foo.FooActor')

    def test03_verifyFooActorNotAvailableWithoutModuleQualifiers(self, asys, source_zips):
        thesplog('ttd %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash = self._loadFooSource(asys, source_zips)
        # The FooActor is not available without module qualifiers even if proper hash is specified
        raises(InvalidActorSpecification,
               asys.createActor, 'FooActor',
               sourceHash = srchash)

    def test04_verifyReloadOfChangedModuleAllowsBothToExistSimultaneously(self, asys, source_zips):
        thesplog('tte %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash = self._loadFooSource(asys, source_zips)
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, ('discard', 'great'))
        assert 'And MOO: great' == r

        # Update the foo sources
        foo2zipFname = os.path.join(tmpdir, 'foo2src.zip')
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
        srchash2 = asys.loadActorSource(foo2zipFname)
        assert srchash2 is not None
        assert srchash != srchash2
        sourceload_wait() # allow time for validation of source by source authority

        foo2 = asys.createActor('foo.FooActor', sourceHash=srchash2)
        r = asys.ask(foo2, 'good one', ask_wait)
        assert 'TOG: good one' == r
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo2, ('discard', 'great'), ask_wait)
        assert '& MOO: great' == r
        r = asys.ask(foo, ('discard', 'great'), ask_wait)
        assert 'And MOO: great' == r

        r = asys.ask(foo, 1, ask_wait)
        assert 'COW: Moooo on 1' == r
        r = asys.ask(foo2, 1, ask_wait)
        assert 'COW: Moooo on 1' == r

    def test04_verifyMultipleSeparateModulesLoaded(self, asys, source_zips):
        thesplog('ttf %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash2 = asys.loadActorSource(dogzipFname)
        srchash = self._loadFooSource(asys, source_zips)
        assert srchash2 is not None
        sourceload_wait() # allow time for validation of source by source authority
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        dog = asys.createActor('dog.DogActor', sourceHash=srchash2)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, ('discard', 'great'))
        assert 'And MOO: great' == r
        r = asys.ask(dog, 'bark', ask_wait)
        assert 'Woof! bark' == r

    def test04_verifyNotificationOfMultipleSeparateModulesLoaded(self, asys, source_zips):
        thesplog('ttf %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        loadnotify = self._registerLoadNotifications(asys)
        srchash2 = asys.loadActorSource(dogzipFname)
        srchash = self._loadFooSource(asys, source_zips)
        assert srchash2 is not None
        sourceload_wait() # allow time for validation of source by source authority
        self._verifyHashNotification(asys, loadnotify, srchash)
        self._verifyHashNotification(asys, loadnotify, srchash2)
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        dog = asys.createActor('dog.DogActor', sourceHash=srchash2)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, ('discard', 'great'))
        assert 'And MOO: great' == r
        r = asys.ask(dog, 'bark', ask_wait)
        assert 'Woof! bark' == r

    def test04_verifyNotificationOfMultipleSeparateModulesLoadedPreAndPostRegistration(self, asys, source_zips):
        thesplog('ttf %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        loadnotify = self._registerLoadNotifications(asys)
        srchash2 = asys.loadActorSource(dogzipFname)
        assert srchash2 is not None
        self._verifyHashNotification(asys, loadnotify, srchash2)
        srchash = self._loadFooSource(asys, source_zips)
        sourceload_wait() # allow time for validation of source by source authority
        self._verifyHashNotification(asys, loadnotify, srchash)
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        dog = asys.createActor('dog.DogActor', sourceHash=srchash2)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, ('discard', 'great'))
        assert 'And MOO: great' == r
        r = asys.ask(dog, 'bark', ask_wait)
        assert 'Woof! bark' == r

    def test04_verifyMultipleSeparateModulesRequireCorrectSourceHashOnCreate(self, asys, source_zips):
        thesplog('ttg %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash2 = asys.loadActorSource(dogzipFname)
        srchash = self._loadFooSource(asys, source_zips)
        assert srchash2 is not None
        sourceload_wait() # allow time for validation of source by source authority
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        raises(ImportError, asys.createActor,
               'dog.DogActor', sourceHash=srchash)  # wrong source hash
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r

    def test05_verifyUnloadOfHashedSourcePreventsActorCreation(self, asys, source_zips):
        thesplog('tth %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash = self._loadFooSource(asys, source_zips)
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, ('discard', 'great'), ask_wait)
        assert 'And MOO: great' == r
        # Unload fooSource
        asys.unloadActorSource(srchash)
        sourceload_wait() # allow time for source unload
        # Test cannot create actors anymore
        raises(InvalidActorSourceHash,
               asys.createActor,
               'foo.FooActor', sourceHash = srchash)

    def test04_actorsCanLoadAndUnloadSource(self, asys, source_zips):
        thesplog('tti %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        # Load first source
        srchash = asys.loadActorSource(dogzipFname)
        import time
        sourceload_wait() # allow time for validation of source by source authority
        # Create an actor from that loaded source
        dogActor = asys.createActor('dog.DogActor', sourceHash = srchash)
        # Verify that actor can be used
        r = asys.ask(dogActor, 'tick', ask_wait)
        assert r == 'Woof! tick'
        # Now ask that actor to load and use a different source
        r = asys.ask(dogActor, [foozipFname, 'barn.cow.moo.MooActor', 'tock'],
                     5)
        assert r == 'And MOO: Bark! tock'


    def test04_verifyNotificationOfMultipleSeparateModulesUnLoaded(self, asys, source_zips):
        thesplog('ttf %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        loadnotify = self._registerLoadNotifications(asys)
        srchash2 = asys.loadActorSource(dogzipFname)
        assert srchash2 is not None
        self._verifyHashNotification(asys, loadnotify, srchash2)
        srchash = self._loadFooSource(asys, source_zips)
        sourceload_wait() # allow time for validation of source by source authority
        self._verifyHashNotification(asys, loadnotify, srchash)
        asys.unloadActorSource(srchash2)
        self._verifyHashNotification(asys, loadnotify, srchash2, False)
        self._verifyHashNotification(asys, loadnotify, srchash)
        srchash3 = asys.loadActorSource(dogzipFname)
        # n.b. srchash2 and srchash3 are probably equal, so cannot verify srchash2
        self._verifyHashNotification(asys, loadnotify, srchash)
        self._verifyHashNotification(asys, loadnotify, srchash3)
        asys.unloadActorSource(srchash)
        self._verifyHashNotification(asys, loadnotify, srchash, False)
        self._verifyHashNotification(asys, loadnotify, srchash3)
        asys.unloadActorSource(srchash3)
        self._verifyHashNotification(asys, loadnotify, srchash, False)
        self._verifyHashNotification(asys, loadnotify, srchash3, False)


    def test04_circular_import_references(self, asys, source_zips):
        thesplog('ttj %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash = self._loadFooSource(asys, source_zips)
        serpent = asys.createActor('ouroboros.Serpent', sourceHash=srchash)
        r = asys.ask(serpent, '-^-v-', ask_wait)
        assert '<<endless-^-v-dragon>>' == r


    def test05_verifyUnloadOfHashedSourceDoesNotKillActiveActors(self, asys, source_zips):
        thesplog('ttk %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash = self._loadFooSource(asys, source_zips)
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 8.3, ask_wait)
        assert 12.1 == round(r, 2)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, ('discard', 'great'), ask_wait)
        assert 'And MOO: great' == r
        # Unload fooSource
        asys.unloadActorSource(srchash)
        # Test foo actors still exist
        r = asys.ask(foo, 10.1, ask_wait)
        assert 13.9 == round(r, 2)
        # Note: behavioral difference here between ActorSystems using
        # the local process memory (e.g. simpleSystemBase) and
        # ActorSystems using remote processes (e.g. multiprocTCPBase,
        # multiprocUDPBase).  The former will have fully unloaded the
        # module and so the imports attempted by foo will fail,
        # whereas the latter affects only *new* Actor processes, but
        # not existing processes, so the existing processes will still have the imports available.
        if asys.base_name in ['simpleSystemBase']:
            r = asys.ask(foo, 'good one', ask_wait)
            assert isinstance(r, PoisonMessage)
        else:
            r = asys.ask(foo, 'good one', ask_wait)
            assert 'GOT: good one' == r
        r = asys.ask(foo, ('discard', 'great'), 3)
        assert 'And MOO: great' == r


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

    # def test05_verifyUnloadOfHashedSourceDoesNotAllowNewSubActorsToBeCreated(self, asys, source_zips):
    #     tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
    #     srchash = self._loadFooSource(asys, source_zips)
    #     foo = asys.createActor('foo.FooActor', sourceHash=srchash)
    #     r = asys.ask(foo, 'good one', ask_wait)
    #     assert 'GOT: good one' == r
    #     # Unload fooSource
    #     asys.unloadActorSource(srchash)
    #     # Verify cannot create sub-actor from still-existing actor from unloaded source

    #     # First, show that an attempt to create an actor that was in
    #     # the removed source gives back an actorAddress to the
    #     # creating Actor, but the actor is never created and thus the
    #     # ask will timeout.
    #     r = asys.ask(foo, ('discard', 'great'), 0.25)
    #     assert r is None

    #     # Now show that an attempt to get a running actor to import a
    #     # module that has been unloaded will cause that Actor to fail
    #     # on the import, resulting in a PoisonMessage indication.
    #     r = asys.ask(foo, 1, 0.25)
    #     assert isinstance(r, PoisonMessage)
    #     assert r.poisonMessage == 1

    def test05_verifyMultipleSeparateModulesCanUseOtherAfterFirstUnloaded(self, asys, source_zips):
        thesplog('ttl %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash2 = asys.loadActorSource(dogzipFname)
        srchash = self._loadFooSource(asys, source_zips)
        assert srchash2 is not None
        asys.unloadActorSource(srchash)
        sourceload_wait() # allow unload to finish
        raises(InvalidActorSourceHash,
               asys.createActor,
               'foo.FooActor', sourceHash=srchash)
        dog = asys.createActor('dog.DogActor', sourceHash=srchash2)
        r = asys.ask(dog, 'bark', ask_wait)
        assert 'Woof! bark' == r

    def test04_verifyReloadOfChangedModuleAndUnloadOfOriginal(self, asys, source_zips):
        thesplog('ttm %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        self._registerSA(asys)
        srchash = self._loadFooSource(asys, source_zips)
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, ('discard', 'great'))
        assert 'And MOO: great' == r

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
        srchash2 = asys.loadActorSource(foo2zipSource)
        assert srchash2 is not None
        assert srchash != srchash2
        sourceload_wait() # allow time for validation of source by source authority

        asys.unloadActorSource(srchash)
        asys.tell(foo, ActorExitRequest())

        foo2 = asys.createActor('foo.FooActor', sourceHash=srchash2)
        raises(InvalidActorSourceHash,
               asys.createActor,
               'foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo2, 'good one', ask_wait)
        assert 'TOG: good one' == r
        r = asys.ask(foo2, ('discard', 'great'), ask_wait)
        assert '& MOO: great' == r

        r = asys.ask(foo2, 1, ask_wait)
        assert 'COW: Moooo on 1' == r

    def test06_sourceAuthorityCanRegister(self, asys):
        thesplog('ttn %s', asys.port_num)
        auth = asys.createActor(rot13Authority)
        enabled = asys.ask(auth, 'Enable', ask_wait)
        assert enabled == 'Enabled'

    def test07_sourceAuthorityRejectsInvalidSource(self, asys, source_zips):
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        auth = asys.createActor(rot13Authority)
        enabled = asys.ask(auth, 'Enable', ask_wait)
        assert enabled == 'Enabled'
        sourceauthority_reg_wait()
        srchash = self._loadFooSource(asys, source_zips)
        assert srchash is not None
        sourceload_wait() # allow time for validation of source by source authority
        raises(InvalidActorSourceHash,
               asys.createActor,
               'foo.FooActor', sourceHash = srchash)

    def test07_sourceAuthorityAcceptsValidSource(self, asys, source_zips):
        thesplog('tto %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        auth = asys.createActor(rot13Authority)
        enabled = asys.ask(auth, 'Enable', ask_wait)
        assert enabled == 'Enabled'
        sourceauthority_reg_wait()
        srchash = asys.loadActorSource(foozipEncFile)
        assert srchash is not None
        sourceload_wait() # allow time for validation of source by source authority
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, ('discard', 'great'))
        assert 'And MOO: great' == r

    def test07_sourceAuthorityAcceptsValidSourceAfterBadSource(self, asys, source_zips):
        thesplog('ttp %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        auth = asys.createActor(rot13Authority)
        enabled = asys.ask(auth, 'Enable', ask_wait)
        assert enabled == 'Enabled'
        sourceauthority_reg_wait()
        failhash = asys.loadActorSource(dogzipFname)
        srchash = asys.loadActorSource(foozipEncFile)
        assert srchash is not None
        sourceload_wait() # allow time for validation of source by source authority
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, ('discard', 'great'))
        assert 'And MOO: great' == r
        raises(InvalidActorSourceHash,
               asys.createActor,
               'dog.DogActor', sourceHash = failhash)

    def test07_sourceAuthorityAcceptsMultipleValidSources(self, asys, source_zips):
        thesplog('ttq %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        auth = asys.createActor(rot13Authority)
        enabled = asys.ask(auth, 'Enable', ask_wait)
        assert enabled == 'Enabled'
        sourceauthority_reg_wait()
        srchash = asys.loadActorSource(foozipEncFile)
        assert srchash is not None
        srchash2 = asys.loadActorSource(dogzipEncFile)
        assert srchash2 is not None
        sourceload_wait() # allow time for validation of source by source authority
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        dog = asys.createActor('dog.DogActor', sourceHash=srchash2)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, ('discard', 'great'))
        assert 'And MOO: great' == r
        r = asys.ask(dog, 'bark', ask_wait)
        assert 'Woof! bark' == r

    def test07_multipleValidSourcesCanCommunicate(self, asys, source_zips):
        thesplog('ttr %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        auth = asys.createActor(rot13Authority)
        enabled = asys.ask(auth, 'Enable', ask_wait)
        assert enabled == 'Enabled'
        sourceauthority_reg_wait()
        srchash = asys.loadActorSource(foozipEncFile)
        assert srchash is not None
        srchash2 = asys.loadActorSource(dogzipEncFile)
        assert srchash2 is not None
        sourceload_wait() # allow time for validation of source by source authority
        cow = asys.createActor('barn.cow.moo.MooActor', sourceHash=srchash)
        dog = asys.createActor('dog.DogActor', sourceHash=srchash2)
        r = asys.ask(cow, 'good one', ask_wait)
        assert 'Moo: good one' == r
        r = asys.ask(dog, 'good boy', ask_wait)
        assert 'Woof! good boy' == r
        r = asys.ask(dog, ('hungry', cow), ask_wait)
        assert 'And MOO: Ruff Ruff: hungry' == r

    def test07_sourceAuthorityAcceptsValidSourceResultIsCorrupted(self, asys, source_zips):
        thesplog('tts %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        auth = asys.createActor(rot13CorruptAuthority)
        enabled = asys.ask(auth, 'Enable', ask_wait)
        assert enabled == 'Enabled'
        sourceauthority_reg_wait()
        srchash = asys.loadActorSource(foozipEncFile)
        assert srchash is not None
        sourceload_wait() # allow time for validation of source by source authority
        try:
            badfoo = asys.createActor('foo.FooActor', sourceHash = srchash)
            assert not ('Should not get here with (%s)!' % str(badfoo))
        except (InvalidActorSourceHash,
                ImportError,
                InvalidActorSpecification,
                EOFError,
                TypeError):
            assert True  # Valid exceptions for a corrupt source
        except Exception as ex:
            assert '' == 'Invalid exception thrown: %s (%s)'%(str(ex), type(ex))

    def test07_sourceAuthorityExceptions(self, asys, source_zips):
        if asys.base_name != 'simpleSystemBase':
            pytest.skip('Source load timeout is too long to wait for valid testing.')
        thesplog('ttt %s', asys.port_num)
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        auth = asys.createActor(rot13FailAuthority)
        enabled = asys.ask(auth, 'Enable', ask_wait)
        assert enabled == 'Enabled'
        sourceauthority_reg_wait()
        srchash = asys.loadActorSource(foozipEncFile)
        assert srchash is not None
        sourceload_wait() # allow time for validation of source by source authority
        raises(InvalidActorSourceHash,
               asys.createActor,
               'foo.FooActor', sourceHash = srchash)

class rot13Authority(Actor):
    def receiveMessage(self, msg, sender):
        if msg == 'Enable':
            self.registerSourceAuthority()
            self.send(sender, 'Enabled')
        elif isinstance(msg, ValidateSource):
            clear = _decryptROT13(msg.sourceData)
            # clear will be None on decryption failure, this actively
            # rejects the source load.
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


class TestFuncMultipleSystemsLoadSource(object):

    def test00_systemsRunnable(self, asys_pair):
        pass

    def test09_properErrorIfActorCapabilitiesNotSatisfied(self, asys_pair, source_zips):
        asys, asys2 = asys_pair
        thesplog('ttu %s %s', asys.port_num, asys2.port_num)
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        asys.updateCapability('Foo Allowed', None)
        asys.updateCapability('Cows Allowed', None)
        asys.updateCapability('Dogs Allowed', None)
        # Specify a Source Authority and load the foo sources
        auth = asys.createActor(rot13Authority)
        enabled = asys.ask(auth, 'Enable', ask_wait)
        assert enabled == 'Enabled'
        sourceauthority_reg_wait()
        srchash = asys.loadActorSource(foozipEncFile)
        assert srchash is not None
        # Establish capabilities that allow Foo and Moo actors (in different systems)
        sourceload_wait() # allow time for validation of source by source authority
        raises(NoCompatibleSystemForActor,
                          asys.createActor,
                          'fish.FishActor', sourceHash=srchash)

    def test08_multiSystemSharesLoadedSourcesByDefault(self, asys_pair, source_zips):
        asys, asys2 = asys_pair
        thesplog('ttv %s %s', asys.port_num, asys2.port_num)
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        asys.updateCapability('Foo Allowed', None)
        asys.updateCapability('Cows Allowed', None)
        asys.updateCapability('Dogs Allowed', None)
        # Specify a Source Authority and load the foo sources
        auth = asys.createActor(rot13Authority)
        enabled = asys.ask(auth, 'Enable', ask_wait)
        assert enabled == 'Enabled'
        sourceauthority_reg_wait()
        srchash = asys.loadActorSource(foozipEncFile)
        assert srchash is not None
        asys.updateCapability('Foo Allowed', True)
        asys2.updateCapability('Cows Allowed', True)
        # Establish capabilities that allow Foo and Moo actors (in different systems)
        sourceload_wait() # allow time for validation of source by source authority

        # Verify that FooActor can be created (locally) and it can
        # create MooActor remotely, where the remote system will
        # obtain the sources from the local system as needed.
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, ('discard', 'great'), ask_wait)
        assert 'And MOO: great' == r

    def test08_loadableInRemoteSystemMatchingCapabilities(self, asys_pair, source_zips):
        asys, asys2 = asys_pair
        thesplog('ttw %s %s', asys.port_num, asys2.port_num)
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        asys.updateCapability('Foo Allowed', None)
        asys.updateCapability('Cows Allowed', None)
        asys.updateCapability('Dogs Allowed', None)
        # Specify a Source Authority and load the foo sources
        auth = asys.createActor(rot13Authority)
        enabled = asys.ask(auth, 'Enable', ask_wait)
        assert enabled == 'Enabled'
        sourceauthority_reg_wait()
        srchash = asys.loadActorSource(foozipEncFile)
        assert srchash is not None
        asys2.updateCapability('Foo Allowed', True)
        asys.updateCapability('Cows Allowed', True)
        # Establish capabilities that allow Foo and Moo actors (in different systems)
        sourceload_wait() # allow time for validation of source by source authority

        # Verify that FooActor can be created (locally) and it can
        # create MooActor remotely, where the remote system will
        # obtain the sources from the local system as needed.
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, ('discard', 'great'), ask_wait)
        assert 'And MOO: great' == r

    def test08_multiSystemSharesLoadedSourcesIfExplicitlyAllowed(self, asys_pair, source_zips):
        asys, asys2 = asys_pair
        thesplog('ttx %s %s', asys.port_num, asys2.port_num)
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        asys.updateCapability('Foo Allowed', None)
        asys.updateCapability('Cows Allowed', None)
        asys.updateCapability('Dogs Allowed', None)
        # Specify a Source Authority and load the foo sources
        auth = asys.createActor(rot13Authority)
        enabled = asys.ask(auth, 'Enable', ask_wait)
        assert enabled == 'Enabled'
        sourceauthority_reg_wait()
        srchash = asys.loadActorSource(foozipEncFile)
        assert srchash is not None
        asys.updateCapability('Foo Allowed', True)
        asys2.updateCapability('Cows Allowed', True)
        asys2.updateCapability('AllowRemoteActorSources', 'yes')
        time.sleep(0.5)  # Allow for Hysteresis delay of two updates from system Two
        # Establish capabilities that allow Foo and Moo actors (in different systems)
        sourceload_wait() # allow time for validation of source by source authority

        # Verify that FooActor can be created (locally) and it can
        # create MooActor remotely, where the remote system will
        # obtain the sources from the local system as needed.
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, ('discard', 'great'), ask_wait)
        assert 'And MOO: great' == r

    def test08_multiSystemLoadedSourcesNotSharedIfExplicitlyDisallowed(self, asys_pair, source_zips):
        asys, asys2 = asys_pair
        thesplog('tty %s %s', asys.port_num, asys2.port_num)
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        asys.updateCapability('Foo Allowed', None)
        asys.updateCapability('Cows Allowed', None)
        asys.updateCapability('Dogs Allowed', None)
        # Specify a Source Authority and load the foo sources
        auth = asys.createActor(rot13Authority)
        enabled = asys.ask(auth, 'Enable', ask_wait)
        assert enabled == 'Enabled'
        sourceauthority_reg_wait()
        srchash = asys.loadActorSource(foozipEncFile)
        assert srchash is not None
        asys.updateCapability('Foo Allowed', True)
        asys2.updateCapability('Cows Allowed', True)
        asys2.updateCapability('AllowRemoteActorSources', 'no')
        # Establish capabilities that allow Foo and Moo actors (in different systems)
        sourceload_wait() # allow time for validation of source by source authority

        # Verify that FooActor can be created (locally) and it can
        # create MooActor remotely, where the remote system will
        # obtain the sources from the local system as needed.
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, ('discard', 'great'), ask_wait)
        assert 'FAILED (poisonous)' == r

    def test08_multiSystemLoadedSourcesNotSharedIfSharingUnrecognized(self, asys_pair, source_zips):
        asys, asys2 = asys_pair
        thesplog('ttz %s %s', asys.port_num, asys2.port_num)
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        asys.updateCapability('Foo Allowed', None)
        asys.updateCapability('Cows Allowed', None)
        asys.updateCapability('Dogs Allowed', None)
        # Specify a Source Authority and load the foo sources
        auth = asys.createActor(rot13Authority)
        enabled = asys.ask(auth, 'Enable', ask_wait)
        assert enabled == 'Enabled'
        sourceauthority_reg_wait()
        srchash = asys.loadActorSource(foozipEncFile)
        assert srchash is not None
        asys.updateCapability('Foo Allowed', True)
        asys2.updateCapability('Cows Allowed', True)
        asys2.updateCapability('AllowRemoteActorSources', 'whatever!')
        # Establish capabilities that allow Foo and Moo actors (in different systems)
        sourceload_wait() # allow time for validation of source by source authority

        # Verify that FooActor can be created (locally) and it can
        # create MooActor remotely, where the remote system will
        # obtain the sources from the local system as needed.
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, ('discard', 'great'), ask_wait)
        assert 'FAILED (poisonous)' == r

    def test08_loadableInRemoteSystemOnlyIfSourceComesFromConventionLeader(self, asys_pair, source_zips):
        asys, asys2 = asys_pair
        thesplog('ttA %s %s', asys.port_num, asys2.port_num)
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        asys.updateCapability('Foo Allowed', None)
        asys.updateCapability('Cows Allowed', None)
        asys.updateCapability('Dogs Allowed', None)
        auth = asys2.createActor(rot13Authority)
        enabled = asys2.ask(auth, 'Enable', ask_wait)
        sourceauthority_reg_wait()
        assert enabled == 'Enabled'
        sourceauthority_reg_wait()
        srchash = asys2.loadActorSource(foozipEncFile)
        assert srchash is not None
        time.sleep(0.1)

        asys2.updateCapability('Foo Allowed', True)
        asys.updateCapability('Cows Allowed', True)
        asys.updateCapability('AllowRemoteActorSources', 'LeaderOnly')
        asys2.updateCapability('AllowRemoteActorSources', 'LeaderOnly')
        # Establish capabilities that allow Foo and Moo actors (in different systems)
        sourceload_wait() # allow time for validation of source by source authority

        raises(InvalidActorSourceHash,
                          asys.createActor,  # KWQ: should be asys2??
                          'foo.FooActor', sourceHash=srchash)

        asys.updateCapability('AllowRemoteActorSources', 'yes')
        time.sleep(0.08)
        foo = asys2.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, ('discard', 'great'), ask_wait)
        assert 'And MOO: great' == r

    def test09_loadableInRemoteSystemUnloadedOnPrimaryUnload(self, asys_pair, source_zips):
        asys, asys2 = asys_pair
        thesplog('ttB %s %s', asys.port_num, asys2.port_num)
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        asys.updateCapability('Foo Allowed', None)
        asys.updateCapability('Cows Allowed', None)
        asys.updateCapability('Dogs Allowed', None)
        auth = asys.createActor(rot13Authority)
        enabled = asys.ask(auth, 'Enable', ask_wait)
        assert enabled == 'Enabled'
        sourceauthority_reg_wait()
        srchash = asys.loadActorSource(foozipEncFile)
        assert srchash is not None
        sourceload_wait() # allow time for validation of source by source authority

        asys2.updateCapability('Foo Allowed', True)
        asys.updateCapability('Cows Allowed', True)
        time.sleep(0.2) # Allow updates to propagate

        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, ('discard', 'great'), ask_wait)
        assert 'And MOO: great' == r
        foo2 = asys2.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo2, 'another good one', ask_wait)
        assert 'GOT: another good one' == r

        # Now unload source and kill actors; they cannot be recreated on either system.
        asys.tell(foo, ActorExitRequest())
        asys.tell(foo2, ActorExitRequest())
        asys.unloadActorSource(srchash)
        sourceunload_wait() # allow time for propagation

        raises(InvalidActorSourceHash,
                          asys.createActor, 'foo.FooActor', sourceHash=srchash)
        raises(InvalidActorSourceHash,
                          asys2.createActor, 'foo.FooActor', sourceHash=srchash)


    def test09_loadableInRemoteSystemUnloadedOnMemberUnload(self, asys_pair, source_zips):
        asys, asys2 = asys_pair
        thesplog('ttC %s %s', asys.port_num, asys2.port_num)
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        asys.updateCapability('Foo Allowed', None)
        asys.updateCapability('Cows Allowed', None)
        asys.updateCapability('Dogs Allowed', None)
        auth = asys.createActor(rot13Authority)
        enabled = asys.ask(auth, 'Enable', ask_wait)
        assert enabled == 'Enabled'
        sourceauthority_reg_wait()
        srchash = asys.loadActorSource(foozipEncFile)
        assert srchash is not None
        sourceload_wait() # allow time for validation of source by source authority

        asys2.updateCapability('Foo Allowed', True)
        asys.updateCapability('Cows Allowed', True)
        time.sleep(0.8) # Allow updates to propagate

        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, ('discard', 'great'), ask_wait)
        assert 'And MOO: great' == r
        foo2 = asys2.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo2, 'another good one', ask_wait)
        assert 'GOT: another good one' == r

        # Now unload source and kill actors; they cannot be recreated on either system.
        asys.tell(foo, ActorExitRequest())
        asys.tell(foo2, ActorExitRequest())
        asys2.unloadActorSource(srchash)

        raises(InvalidActorSourceHash,
                          asys.createActor, 'foo.FooActor', sourceHash=srchash)
        raises(InvalidActorSourceHash,
                          asys2.createActor, 'foo.FooActor', sourceHash=srchash)


    def test10_multiSystemStartSubActorByClassReferenceLocally(self, asys_pair, source_zips):
        asys, asys2 = asys_pair
        thesplog('ttD %s %s', asys.port_num, asys2.port_num)
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        asys.updateCapability('Foo Allowed', None)
        asys.updateCapability('Cows Allowed', None)
        asys.updateCapability('Dogs Allowed', None)
        auth = asys.createActor(rot13Authority)
        enabled = asys.ask(auth, 'Enable', ask_wait)
        assert enabled == 'Enabled'
        sourceauthority_reg_wait()
        srchash = asys.loadActorSource(foozipEncFile)
        assert srchash is not None
        asys.updateCapability('Foo Allowed', True)
        asys2.updateCapability('Cows Allowed', True)
        # Establish capabilities that allow Foo and Moo actors (in different systems)
        sourceload_wait() # allow time for validation of source by source authority

        # Verify that FooActor can be created (locally) and it can
        # create MooActor remotely, where the remote system will
        # obtain the sources from the local system as needed.
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, [{'Foo Allowed': True}, 'Where Pigs Fly'], ask_wait)
        assert 'In a WORLD: Where Pigs Fly' == r


    def test10_multiSystemStartSubActorByClassReferenceRemotely(self, asys_pair, source_zips):
        asys, asys2 = asys_pair
        thesplog('ttE %s %s', asys.port_num, asys2.port_num)
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        asys.updateCapability('Foo Allowed', None)
        asys.updateCapability('Cows Allowed', None)
        asys.updateCapability('Dogs Allowed', None)
        auth = asys.createActor(rot13Authority)
        enabled = asys.ask(auth, 'Enable', ask_wait)
        assert enabled == 'Enabled'
        sourceauthority_reg_wait()
        srchash = asys.loadActorSource(foozipEncFile)
        assert srchash is not None
        asys.updateCapability('Foo Allowed', True)
        asys2.updateCapability('Cows Allowed', True)
        # Establish capabilities that allow Foo and Moo actors (in different systems)
        sourceload_wait() # allow time for validation of source by source authority

        # Verify that FooActor can be created (locally) and it can
        # create MooActor remotely, where the remote system will
        # obtain the sources from the local system as needed.
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, [{'Cows Allowed': True}, 'Where Pigs Fly'], ask_wait)
        assert 'In a WORLD: Where Pigs Fly' == r


    def test10_multiSystemStartSubActorCannotStart(self, asys_pair, source_zips):
        asys, asys2 = asys_pair
        thesplog('ttF %s %s', asys.port_num, asys2.port_num)
        actor_system_unsupported(asys, 'simpleSystemBase', 'multiprocQueueBase')
        tmpdir, foozipFname, foozipEncFile, dogzipFname, dogzipEncFile = source_zips
        asys.updateCapability('Foo Allowed', None)
        asys.updateCapability('Cows Allowed', None)
        asys.updateCapability('Dogs Allowed', None)
        auth = asys.createActor(rot13Authority)
        enabled = asys.ask(auth, 'Enable', ask_wait)
        assert enabled == 'Enabled'
        sourceauthority_reg_wait()
        srchash = asys.loadActorSource(foozipEncFile)
        assert srchash is not None
        asys.updateCapability('Foo Allowed', True)
        asys2.updateCapability('Cows Allowed', True)
        # Establish capabilities that allow Foo and Moo actors (in different systems)
        sourceload_wait() # allow time for validation of source by source authority

        # Verify that FooActor can be created (locally) and it can
        # create MooActor remotely, where the remote system will
        # obtain the sources from the local system as needed.
        foo = asys.createActor('foo.FooActor', sourceHash=srchash)
        r = asys.ask(foo, 'good one', ask_wait)
        assert 'GOT: good one' == r
        r = asys.ask(foo, [{'Elephants Allowed': True}, 'Where Pigs Fly'], ask_wait)
        assert 'FAILED (poisonous)' == r
