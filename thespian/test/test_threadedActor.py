import logging
import time, datetime
from thespian.test import *
from thespian.actors import *
import threading
from thespian.system.utilis import thesplog


ASK_WAIT = datetime.timedelta(seconds=20)

def threaded(count):
    """This is the main body of each thread.  It will generate a `count`
       number of log messages (which will cause actor transmits to the
       logger) and then exit.
    """
    try:
        time.sleep(1)
        for x in range(count):
            logging.debug('Msg %s of %s', x, count)
            time.sleep(0.0001)
        time.sleep(1)
        logging.debug('Done')
    except Exception as ex:
        thesplog('Failed threading because: %s', ex)
        logging.exception('Failed threading')


class Weaver(ActorTypeDispatcher):
    """This is the main actor that will create a number of threads, then
       wait for the threads to complete.

       Note that this actor blocks while waiting for all the thread
       activity, so it is not available to provide actor receive
       functionality during this time.  If enough threads are started,
       there can be a very large number of transmits (exceeding
       internal overflow thresholds) unless the multi-threading works
       properly to allow those transmits to be sent.
    """

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

@pytest.mark.parametrize('num_threads', [1,10,30,100])
def test_threads(asys, num_threads):
        weaver = asys.createActor(Weaver)
        r = asys.ask(weaver, num_threads, ASK_WAIT)
        assert r == 'done'
