import logging
import time, datetime
from thespian.test import *
from thespian.actors import *
import threading
from thespian.system.utilis import thesplog


ASK_WAIT = datetime.timedelta(seconds=15)


def threaded(count):
    try:
        thesplog('threaded with %s', count)
        time.sleep(1)
        for x in range(count):
            thesplog('threaded number %s of %s', x, count)
            logging.debug('Msg %s of %s', x, count)
            time.sleep(0.01)
        thesplog('done with %s', count)
        time.sleep(1)
        logging.debug('Done')
        thesplog('gone')
    except Exception as ex:
        thesplog('Failed threading because: %s', ex)


class Weaver(ActorTypeDispatcher):

    def receiveMsg_int(self, intmsg, sender):
        threads = []
        for x in range(intmsg):
            threads.append(threading.Thread(target=threaded, args=(5,)))
        logging.debug('Threads created: %s', intmsg)
        for x in range(intmsg):
            threads[x].start()
        logging.debug('Threads started: %s', intmsg)
        for x in range(intmsg):
            for y in range(10):
                logging.debug('Joining thread %s, attempt %s', x, y)
                threads[x].join(timeout=0.5)
                if not threads[x].is_alive():
                    logging.debug('Thread %s finished (%s checks)', x, y)
                    break
                else:
                    logging.debug('Thread %s not done (check %s)', x, y)
            else:
                logging.warning('Thread %s did not complete!', x)
        logging.debug('All done with threads')
        self.send(sender, 'done')



class TestFuncThreadedActor(object):
    def testCreateActorSystem(self, asys): pass

    def test_one_thread(self, asys):
        weaver = asys.createActor(Weaver)
        r = asys.ask(weaver, 1, ASK_WAIT)
        assert r == 'done'

    def test_ten_threads(self, asys):
        weaver = asys.createActor(Weaver)
        r = asys.ask(weaver, 10, ASK_WAIT)
        assert r == 'done'

