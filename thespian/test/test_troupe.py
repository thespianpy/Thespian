import time
import datetime
from thespian.test import *
from thespian.actors import *
from thespian.troupe import troupe


max_listen_wait = datetime.timedelta(seconds=4)
max_ask_wait    = datetime.timedelta(seconds=2.5)


class Bee(Actor):
    def receiveMessage(self, msg, sender):
        if isinstance(msg, tuple):
            time.sleep(msg[0])
            self.send(sender, msg[1] + ' buzz')


@troupe()
class Hive(Bee):
    pass


@troupe()
class Colony(ActorTypeDispatcher):
    def receiveMsg_tuple(self, msg, sender):
        if not hasattr(self, 'hive'):
            self.hive = self.createActor(Hive)
            self.asker = []
        self.asker.append(sender)
        self.send(self.hive, msg)
        self.troupe_work_in_progress = True

    def receiveMsg_str(self, msg, sender):
        self.send(self.asker.pop(), msg)
        self.troupe_work_in_progress = bool(getattr(self, 'asker', False))

# Ensure there are more test data elements than workers so that
# some workers get multiple messages
testdata = [(0.5, 'Fizz'), (1, 'Honey'),
            (0.25, 'Flower'), (0.75, 'Pollen'),
           ] + ([(0.005, 'Orchid'), (0.005, 'Rose'),
                 (0.005, 'Carnation'), (0.005, 'Lily'),
                 (0.005, 'Daffodil'), (0.005, 'Begonia'),
                 (0.005, 'Violet'), (0.005, 'Aster'),
           ] * 3)


def useActorForTest(asys, bee):
    # Run multiple passes to allow workers to be reaped between passes
    for X in range(2):
        print(X)
        for each in testdata:
            asys.tell(bee, each)
        remaining = testdata[:]
        for readnum in range(len(testdata)):
            rsp = asys.listen(max_listen_wait)
            assert rsp
            print(str(rsp))
            remaining = [R for R in remaining
                         if not rsp.startswith(R[1])]
        assert not remaining
    asys.tell(bee, ActorExitRequest())


def testSingleBee(asys):
    useActorForTest(asys, asys.createActor(Bee))


def testHive(asys):
    useActorForTest(asys, asys.createActor(Hive))


def testColony(asys):
    useActorForTest(asys, asys.createActor(Colony))

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

@troupe()
class Colony(Bee):
    def receiveMessage(self, msg, sender):
        if isinstance(msg, tuple):
            if not hasattr(self, 'hive'):
                self.hive = self.createActor(Hive)
                self.asker = []
            self.asker.append(sender)
            self.send(self.hive, msg)
            self.troupe_work_in_progress = True
        elif isinstance(msg, str):
            self.send(self.asker.pop(), msg)
            self.troupe_work_in_progress = bool(self.asker)
    ''')
    hivezip.close()
    request.addfinalizer(lambda d=tmpdir:
                         os.path.exists(d) and shutil.rmtree(d))
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
                           sourceHash=srchash)
    useActorForTest(asys, bee)


def testLoadableColony(asys, source_zip):
    r = asys.ask(asys.createActor(SimpleSourceAuthority), 'go', max_ask_wait)
    assert r == 'ok'
    r = asys.ask(asys.createActor(LoadWatcher), 'go', max_ask_wait)
    assert r == 'ok'

    srchash = asys.loadActorSource(source_zip)
    r = asys.listen(max_listen_wait)
    assert r == srchash

    bee = asys.createActor('forest.clearing.beehive.Colony',
                           sourceHash=srchash)
    useActorForTest(asys, bee)
