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


def asker_main(asys, count, finished_address, ordered=False):
    """This is the main body of each sending thread.  It will generate a
       `count` number of ask requests to a target actor.
    """
    global finishes_lock, success_finishes, failure_finishes
    try:
        time.sleep(0.01)
        target = asys.createActor(Computer)
        try:
            for x in range(count):
                r = asys.ask(target, x % 8, ASK_WAIT)
                assert r is not None
                if ordered:
                    assert r == (x % 8) * 2
            with finishes_lock:
                success_finishes += 1
            if finished_address:
                asys.tell(finished_address, 'done')
        finally:
            asys.tell(target, ActorExitRequest())
    except Exception as ex:
        logging.exception('Failed threading')
        with finishes_lock:
            failure_finishes += 1
        if finished_address:
            asys.tell(finished_address, 'done badly')


def context_asker(asystem, count, finished_address=None):
    with (asystem or ActorSystem()).private() as asys:
        return asker_main(asys, count, finished_address, True)



class Computer(ActorTypeDispatcher):

    def receiveMsg_int(self, intmsg, sender):
        time.sleep(0.01 * intmsg)  # I'm working...
        self.send(sender, intmsg * 2)


class Transfer(ActorTypeDispatcher):

    wakeup_period = 0.01  # no processing while sleeping on this

    def __init__(self):
        self.messages = []
        self.querying = None

    def receiveMsg_WakeupMessage(self, wakemsg, sender):
        if self.querying:
            if self.messages:
                self.send(self.querying, self.messages.pop(0))
                self.querying = None
            else:
                self.wakeupAfter(self.wakeup_period)

    def receiveMsg_str(self, msg, sender):
        if msg == 'query':
            if self.messages:
                self.send(sender, self.messages.pop(0))
            else:
                self.querying = sender
                self.wakeupAfter(self.wakeup_period)
        else:
            self.messages.append(msg)

    def receiveMsg_ActorSystemMessage(self, sysmsg, sender):
        pass

    def receiveUnrecognizedMessage(self, msg, sender):
        self.messages.append(msg)


class TestFuncThreadedExternal(object):

    def test_one_thread_private_context(self, asys):
        # The simpleSystemBase is unsupported in this mode because the
        # main thread does not perform any ActorSystem operations, so
        # it never provides the context for the other threads to have
        # their requested work performed.
        actor_system_unsupported(asys, 'simpleSystemBase')
        global finishes_lock, success_finishes, failure_finishes
        starting_successes = success_finishes
        t1 = threading.Thread(target=context_asker, args=(asys, 20))
        t1.start()
        t1.join(timeout=THREAD_WAIT_TIME)
        print('Finishes: %s successful, %s failure' %
              (success_finishes, failure_finishes))
        assert success_finishes == starting_successes + 1


    def test_one_thread_private_context_ask(self, asys):
        global finishes_lock, success_finishes, failure_finishes
        starting_successes = success_finishes
        transfer = asys.createActor(Transfer)
        t1 = threading.Thread(target=context_asker, args=(asys, 20, transfer))
        t1.start()
        time.sleep(0.2)
        for t in range(10):
            r = asys.ask(transfer, 'query', ASK_WAIT)
            if r is not None:
                break
        t1.join(timeout=THREAD_WAIT_TIME)
        print('Finishes: %s successful, %s failure' %
              (success_finishes, failure_finishes))
        assert success_finishes == starting_successes + 1


    @pytest.mark.parametrize('count', [1,2,5,10,100])
    def test_multiple_threads_private_context(self, asys, count):
        # The Queue base has undiagnosed issues with threading shutdown
        unstable_test(asys, 'multiprocQueueBase')
        # The simpleSystemBase is unsupported in this mode because the
        # main thread does not perform any ActorSystem operations, so
        # it never provides the context for the other threads to have
        # their requested work performed.
        actor_system_unsupported(asys, 'simpleSystemBase')
        # UDP does not have delivery confirmation, it is unreliable,
        # especially at higher counts.
        if count > 10:
            unstable_test(asys, 'multiprocUDPBase', 'multiprocQueueBase')
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

    @pytest.mark.parametrize('count', [1,2,5,10,100])
    def test_multiple_threads_private_context_ask(self, asys, count):
        # The Queue base has undiagnosed issues with threading shutdown
        unstable_test(asys, 'multiprocQueueBase')
        # UDP does not have delivery confirmation, it is unreliable,
        # especially at higher counts.
        if count > 10:
            unstable_test(asys, 'multiprocUDPBase')
        global finishes_lock, success_finishes, failure_finishes
        starting_successes = success_finishes
        transfer = asys.createActor(Transfer)
        tl = [threading.Thread(target=context_asker, args=(asys, 20, transfer))
              for C in range(count)]
        for idx,each in enumerate(tl):
            print('#',idx,each,each.name)
        [T.start() for T in tl]
        d = 0
        for t in range(10):
            if asys.ask(transfer, 'query', ASK_WAIT) is not None:
                d = d + 1
                if d == len(tl):
                    break
        [T.join(timeout=THREAD_WAIT_TIME) for T in tl]
        print('Finishes: %s successful, %s failure' %
              (success_finishes, failure_finishes))
        assert success_finishes == starting_successes + count
