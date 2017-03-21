import logging
import time, datetime
from thespian.test import *
from thespian.actors import *
import threading


ASK_WAIT = datetime.timedelta(seconds=5)
THREAD_WAIT_TIME=5 # seconds

finishes_lock = threading.Lock()
success_finishes = 0
failure_finishes = 0


def asker_main(asys, count, ordered=False):
    """This is the main body of each sending thread.  It will generate a
       `count` number of ask requests to a target actor.
    """
    global finishes_lock, success_finishes, failure_finishes
    try:
        time.sleep(0.01)
        target = asys.createActor(Computer)
        for x in range(count):
            r = asys.ask(target, x % 8, ASK_WAIT)
            assert r is not None
            if ordered:
                assert r == (x % 8) * 2
        ActorSystem().tell(target, ActorExitRequest())
        with finishes_lock:
            success_finishes += 1
    except Exception as ex:
        logging.exception('Failed threading')
        with finishes_lock:
            failure_finishes += 1

def asker(asystem, count):
    return asker_main(asystem or ActorSystem(), count, False)


def context_asker(asystem, count):
    with (asystem or ActorSystem()).private() as asys:
        return asker_main(asys, count, True)



class Computer(ActorTypeDispatcher):

    def receiveMsg_int(self, intmsg, sender):
        time.sleep(0.01 * intmsg)
        self.send(sender, intmsg * 2)


class TestFuncThreadedExternal(object):

    def test_one_thread_common_context(self, asys):
        global finishes_lock, success_finishes, failure_finishes
        starting_successes = success_finishes
        t1 = threading.Thread(target=asker, args=(asys, 20))
        t1.start()
        t1.join(timeout=THREAD_WAIT_TIME)
        print('Finishes: %s successful, %s failure' %
              (success_finishes, failure_finishes))
        assert success_finishes == starting_successes + 1


    def test_one_thread_private_context(self, asys):
        global finishes_lock, success_finishes, failure_finishes
        starting_successes = success_finishes
        t1 = threading.Thread(target=context_asker, args=(asys, 20))
        t1.start()
        t1.join(timeout=THREAD_WAIT_TIME)
        print('Finishes: %s successful, %s failure' %
              (success_finishes, failure_finishes))
        assert success_finishes == starting_successes + 1


    @pytest.mark.parametrize('count', [1,2,5]) #,10,100])
    def test_multiple_threads_common_context(self, asys, count):
        global finishes_lock, success_finishes, failure_finishes
        starting_successes = success_finishes
        tl = [threading.Thread(target=asker, args=(asys, 20))
              for C in range(count)]
        for idx,each in enumerate(tl):
            print('#',idx,each,each.name)
        [T.start() for T in tl]
        [T.join(timeout=THREAD_WAIT_TIME) for T in tl]
        print('Finishes: %s successful, %s failure' %
              (success_finishes, failure_finishes))
        assert success_finishes == starting_successes + count


    @pytest.mark.parametrize('count', [1,2,5,10,100])
    def test_multiple_threads_private_context(self, asys, count):
        global finishes_lock, success_finishes, failure_finishes
        starting_successes = success_finishes
        tl = [threading.Thread(target=context_asker, args=(asys, 20))
              for C in range(count)]
        for idx,each in enumerate(tl):
            print('#',idx,each,each.name)
        [T.start() for T in tl]
        [T.join(timeout=THREAD_WAIT_TIME) for T in tl]
        print('Finishes: %s successful, %s failure' %
              (success_finishes, failure_finishes))
        assert success_finishes == starting_successes + count
