import time
from datetime import datetime, timedelta
from pytest import raises
from thespian.test import *
from thespian.actors import *
from thespian.troupe import troupe, UpdateTroupeSettings

max_listen_wait = timedelta(seconds=6.5)
max_ask_wait    = timedelta(seconds=2.5)


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
           ] + ([(0.013, 'Orchid'), (0.010, 'Rose'),
                 (0.013, 'Carnation'), (0.010, 'Lily'),
                 (0.013, 'Daffodil'), (0.010, 'Begonia'),
                 (0.010, 'Violet'), (0.010, 'Aster'),
           ] * 6)


def useActorForTest(asys, bee, minimum_time=None, exit_when_done=True):
    # Run multiple passes to allow workers to be reaped between passes
    start_time = datetime.now()
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
    if exit_when_done:
        asys.tell(bee, ActorExitRequest())
    elapsed = datetime.now() - start_time
    if minimum_time:
        assert elapsed > minimum_time
    else:
        print(str(elapsed))


def testSingleBee(asys, run_unstable_tests):
    unstable_test(run_unstable_tests, asys, 'multiprocQueueBase')
    useActorForTest(asys, asys.createActor(Bee),
                    minimum_time=timedelta(seconds=6))


def testHive(asys, run_unstable_tests):
    print(len(testdata))

    useActorForTest(asys, asys.createActor(Hive),
                    # Default is 10 troupe workers, for 52 work
                    # elements means approximately 5 each.  The worst
                    # time of 1 second for each of 2 runs, plus 4 of
                    # the other test points.  All of the smallest
                    # should be completed quickly, and since the large
                    # test points are only 4, that leaves 6 workers to
                    # quickly complete the majority of the test
                    # points, so the 1 second test for each run is the
                    # dominator.
                    minimum_time=timedelta(seconds=2,
                                           # with a little extra execution time
                                           milliseconds=1))

def testSmallHive_AdjStr(asys, run_unstable_tests):
    print(len(testdata))
    worker = asys.createActor(Hive)
    r = asys.ask(worker, "troupe:status?")
    assert "Max=10," in r
    r = asys.ask(worker, "troupe:set_max_count=nine")
    assert "Error changing max_count" in r
    r = asys.ask(worker, "troupe:set_max_count=1")
    assert "Set troupe max_count to 1" == r
    r = asys.ask(worker, "troupe:status?")
    assert "Max=1," in r
    useActorForTest(asys, worker,
                    # Only 1 worker, so all work handled sequentially
                    minimum_time=timedelta(seconds=6),
                    exit_when_done=False)

    r = asys.ask(worker, "troupe:set_max_count=2")
    assert "Set troupe max_count to 2" == r
    r = asys.ask(worker, "troupe:status?")
    assert "Max=2," in r
    useActorForTest(asys, worker,
                    # Two workers, so the work should take half as long
                    minimum_time=timedelta(seconds=3),
                    exit_when_done=False)


    r = asys.ask(worker, "troupe:set_max_count=52")
    assert "Set troupe max_count to 52" == r
    r = asys.ask(worker, "troupe:status?")
    assert "Max=52," in r
    useActorForTest(asys, worker,
                    # One worker for each work item, so the 1 second work for two passes is the dominator
                    minimum_time=timedelta(seconds=1),
                    exit_when_done=False)

    r = asys.ask(worker, "troupe:set_max_count=1")
    assert "Set troupe max_count to 1" == r
    r = asys.ask(worker, "troupe:set_idle_count=1")
    assert "Set troupe idle_count to 1" == r
    r = asys.ask(worker, "troupe:status?")
    assert "Max=1," in r
    assert "Idle=1," in r

    # Invoke workers, and when each finishes the troupe leader will
    # see that there are more workers than needed and dismiss the
    # workers.  This should reduce down to the idle count, which is
    # reset to 1 (default is 2).
    for each in range(60):
        r = asys.tell(worker, "buzz off")
    time.sleep(2)

    useActorForTest(asys, worker,
                    # Back to 1 worker, so all work handled sequentially
                    minimum_time=timedelta(seconds=6))

def testSmallHive_AdjMsg(asys, run_unstable_tests):
    print(len(testdata))
    worker = asys.createActor(Hive)
    r = asys.ask(worker, "troupe:status?")
    assert "Max=10," in r

    with raises(TypeError) as excinfo:
        r = asys.ask(worker, UpdateTroupeSettings(max_count="nine"))

    r = asys.ask(worker, UpdateTroupeSettings(max_count=1))
    assert isinstance(r, UpdateTroupeSettings)
    assert r.max_count == 1
    assert r.idle_count == 2
    r = asys.ask(worker, "troupe:status?")
    assert "Max=1," in r
    useActorForTest(asys, worker,
                    # Only 1 worker, so all work handled sequentially
                    minimum_time=timedelta(seconds=6),
                    exit_when_done=False)

    r = asys.ask(worker, UpdateTroupeSettings(max_count=2, idle_count=5))
    assert isinstance(r, UpdateTroupeSettings)
    assert r.max_count == 2
    assert r.idle_count == 5
    r = asys.ask(worker, "troupe:status?")
    assert "Max=2," in r
    useActorForTest(asys, worker,
                    # Two workers, so the work should take half as long
                    minimum_time=timedelta(seconds=3),
                    exit_when_done=False)


    # Can mix using strings and UpdateTroupeSettings
    r = asys.ask(worker, "troupe:set_max_count=52")
    assert "Set troupe max_count to 52" == r
    r = asys.ask(worker, "troupe:status?")
    assert "Max=52," in r
    useActorForTest(asys, worker,
                    # One worker for each work item, so the 1 second work for two passes is the dominator
                    minimum_time=timedelta(seconds=1),
                    exit_when_done=False)

    r = asys.ask(worker, UpdateTroupeSettings(max_count=1, idle_count=1))
    assert isinstance(r, UpdateTroupeSettings)
    assert r.max_count == 1
    assert r.idle_count == 1
    r = asys.ask(worker, "troupe:status?")
    assert "Max=1," in r
    assert "Idle=1," in r

    # Invoke workers, and when each finishes the troupe leader will
    # see that there are more workers than needed and dismiss the
    # workers.  This should reduce down to the idle count, which is
    # reset to 1 (default is 2).
    for each in range(60):
        r = asys.tell(worker, "buzz off")
    time.sleep(2)

    useActorForTest(asys, worker,
                    # Back to 1 worker, so all work handled sequentially
                    minimum_time=timedelta(seconds=6))


def testColony(asys, run_unstable_tests):
    unstable_test(run_unstable_tests, asys, 'multiprocQueueBase')
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
