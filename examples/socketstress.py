# Measures the time required to send and receive to/from
# a given number of actors with the intention to
# compare efficiency of different I/O multiplexing
# methods.
#
# Run this from the top level as:
#    $ python examples/socketstress.py [<number-of-workers>] [<number-of-repetitions>]


import logging
import time
from logsetup import logcfg
from datetime import timedelta
from thespian.actors import *

### messages

class BaseMsg(object): pass


class Ping(BaseMsg): pass


class Pong(BaseMsg): pass


class Run(BaseMsg): pass


class Start(BaseMsg):
    def __init__(self, num_workers, num_repetitions):
        self.num_workers = num_workers
        self.num_repetitions = num_repetitions


class WorkerStart(BaseMsg): pass


class WorkerStarted(BaseMsg): pass

### actors

class Dispatcher(ActorTypeDispatcher):
    def __init__(self):
        self.num_workers = 0
        self.workers = []
        self.pong_count = 0
        self.worker_started_count = 0
        self.sender = None
        self.has_completed = False

    def receiveMsg_Start(self, message, sender):
        self.num_workers = message.num_workers
        self.num_repetitions = message.num_repetitions
        self.sender = sender
        logging.info('receiveMsg_Start(): creating %s workers...', self.num_workers)
        for _ in range(self.num_workers):
            worker = self.createActor(Worker)
            self.workers.append(worker)
            self.send(worker, WorkerStart())
        logging.info('receiveMsg_Start(): done', self.num_workers)

    def receiveMsg_Run(self, message, sender):
        logging.info('receiveMsg_Run(): sending pings...')
        self.sender = sender
        for each in self.workers:
            self.send(each, Ping())
        logging.info('receiveMsg_Run(): done')

    def receiveMsg_Pong(self, message, sender):
        self.pong_count += 1
        if self.pong_count >= self.num_workers * self.num_repetitions and not self.has_completed:
            self.has_completed = True
            self.send(self.sender, "done")
        if self.num_repetitions > 1:
            self.send(sender, Ping())

    def receiveMsg_WorkerStarted(self, message, sender):
        self.worker_started_count += 1
        if self.worker_started_count == self.num_workers:
            logging.info('receiveMsg_WorkerStarted(): %s workers started', self.worker_started_count)
            self.send(self.sender, "started")


class Worker(ActorTypeDispatcher):
    def receiveMsg_Ping(self, message, sender):
        self.send(sender, Pong())

    def receiveMsg_WorkerStart(self, message, sender):
        self.send(sender, WorkerStarted())


def run_example(num_workers, num_repetitions):
    try:
        num_workers = int(num_workers)
        num_repetitions = int(num_repetitions)
    except ValueError:
        print('usage: socketstress.py [<num-workers>] [<num-repetitions>]')
        sys.exit(1)
    asys = ActorSystem("multiprocTCPBase", logDefs=logcfg)
    try:
        print(f'socketstress with {num_workers} worker(s) and {num_repetitions} repetition(s)')
        print('creating dispatcher...')
        dispatcher = ActorSystem().createActor(Dispatcher)
        print('starting workers...')
        ActorSystem().ask(dispatcher, Start(num_workers, num_repetitions))
        print('run!')
        start = time.perf_counter()
        ActorSystem().ask(dispatcher, Run())
        end = time.perf_counter()
        print(f'run completed in {end - start} seconds')
    finally:
        asys.shutdown()

if __name__ == "__main__":
    import sys
    run_example(
        sys.argv[1] if len(sys.argv) > 1 else "3",
        sys.argv[2] if len(sys.argv) > 2 else "1"
    )
