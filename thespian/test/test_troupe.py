import logging
import time, datetime
from thespian.test import *
from thespian.actors import *
from thespian.troupe import troupe


max_listen_wait = datetime.timedelta(seconds=2.5)
max_ask_wait    = datetime.timedelta(seconds=2.5)

class Bee(Actor):
    def receiveMessage(self, msg, sender):
        if isinstance(msg, tuple):
            time.sleep(msg[0])
            self.send(sender, msg[1] + ' buzz')


@troupe()
class Hive(Bee): pass

testdata = [ (1, 'Fizz'), (2, 'Honey'),
             (0.5, 'Flower'), (1.5, 'Pollen'),
]

def useActorForTest(asys, bee):
    starttime = datetime.datetime.now()
    for each in testdata:
        asys.tell(bee, each)
    remaining = testdata[:]
    for readnum in range(len(testdata)):
        rsp = asys.listen(max_listen_wait)
        assert rsp
        remaining = [R for R in remaining
                     if not rsp.startswith(R[1])]
    asys.tell(bee, ActorExitRequest())
    assert not remaining

def testSingleBee(asys):
    bee = asys.createActor(Bee)
    useActorForTest(asys, bee)

def testHive(asys):
    bee = asys.createActor(Hive)
    useActorForTest(asys, bee)

# ------------------------------------------------------------

class SimpleSourceAuthority(ActorTypeDispatcher):
    def receiveMsg_str(self, msg, sender):
        self.registerSourceAuthority()
        self.send(sender, 'ok')
    def receiveMsg_ValidateSource(self, msg, sender):
        self.send(sender, ValidatedSource(msg.sourceHash, msg.sourceData))


class LoadWatcher(ActorTypeDispatcher):
    def receiveMsg_str(self, msg, sender):
        if msg == 'go':
            self.notifyOnSourceAvailability(True)
            self._tell = sender
            self.send(sender, 'ok')
        elif msg == 'stop':
            self.notifyOnSourceAvailability(False)
            self._tell = None
    def receiveMsg_LoadedSource(self, loadmsg, sender):
        if getattr(self, '_tell', None):
            self.send(self._tell, loadmsg.sourceHash)
    def receiveMsg_UnloadedSource(self, unloadmsg, sender):
        if getattr(self, '_tell', None):
            self.send(self._tell, ('unloaded', unloadmsg.sourceHash))


import tempfile, zipfile, os, shutil

@pytest.fixture()
def source_zip(request):
    tmpdir = tempfile.mkdtemp()
    zipfname = os.path.join(tmpdir, 'hivesrc.zip')
    hivezip = zipfile.ZipFile(zipfname, 'w')
    hivezip.writestr('__init__.py', '')
    hivezip.writestr('forest/__init__.py', '')
    hivezip.writestr('forest/clearing/__init__.py', '')
    hivezip.writestr('forest/clearing/beehive.py', '''
import time
from thespian.actors import *
from thespian.troupe import troupe

class Bee(Actor):
    def receiveMessage(self, msg, sender):
        if isinstance(msg, tuple):
            time.sleep(msg[0])
            self.send(sender, msg[1] + ' buzz')


@troupe()
class Hive(Bee): pass
    ''')
    hivezip.close()
    request.addfinalizer(lambda d=tmpdir: os.path.exists(d) and shutil.rmtree(d))
    return zipfname


def testLoadableHive(asys, source_zip):
    r = asys.ask(asys.createActor(SimpleSourceAuthority), 'go', max_ask_wait)
    assert r == 'ok'
    r = asys.ask(asys.createActor(LoadWatcher), 'go', max_ask_wait)
    assert r == 'ok'

    srchash = asys.loadActorSource(source_zip)
    r = asys.listen(max_listen_wait)
    assert r == srchash

    bee = asys.createActor('forest.clearing.beehive.Hive',
                           sourceHash = srchash)
    useActorForTest(asys, bee)
