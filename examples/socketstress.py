# Measures the time required to send and receive to/from
# a given number of actors with the intention to
# compare efficiency of different I/O multiplexing
# methods.
#
# Run this from the top level as:
#    $ python examples/socketstress.py [number-of-workers]


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
    def __init__(self, num_workers):
        self.num_workers = num_workers

### actors

class Dispatcher(ActorTypeDispatcher):
    def __init__(self):
        self.num_workers = 0
        self.workers = []
        self.pong_count = 0
        self.run_sender = None

    def receiveMsg_Start(self, message, sender):
        self.num_workers = message.num_workers
        logging.info('receiveMsg_Start(): creating %s workers...', self.num_workers)
        for _ in range(self.num_workers):
            self.workers.append(self.createActor(Worker))
        self.send(sender, "done")
        logging.info('receiveMsg_Start(): done', self.num_workers)

    def receiveMsg_Run(self, message, sender):
        logging.info('receiveMsg_Run(): sending pings...')
        self.run_sender = sender
        for each in self.workers:
            self.send(each, Ping())
        logging.info('receiveMsg_Run(): done')

    def receiveMsg_Pong(self, message, sender):
        self.pong_count += 1
        self.send(sender, Ping())
        if self.pong_count >= self.num_workers * 100:
            self.send(self.run_sender, "done")


class Worker(ActorTypeDispatcher):
    def receiveMsg_Ping(self, message, sender):
        self.send(sender, Pong())


def run_example(num_workers):
    try:
        num_workers = int(num_workers)
    except ValueError:
        print('please specify the number of workers')
        sys.exit(1)
    asys = ActorSystem("multiprocTCPBase", logDefs=logcfg)
    try:
        print('creating dispatcher...')
        dispatcher = ActorSystem().createActor(Dispatcher)
        print('initiating workers start...')
        ActorSystem().ask(dispatcher, Start(num_workers))
        seconds = 3
        print(f'waiting {seconds} seconds...')
        time.sleep(seconds)
        print('run!')
        start = time.perf_counter()
        ActorSystem().ask(dispatcher, Run())
        end = time.perf_counter()
        print(f'run completed in {end - start} seconds')
    finally:
        asys.shutdown()

if __name__ == "__main__":
    import sys
    run_example(sys.argv[1] if len(sys.argv) > 1 else "3")
