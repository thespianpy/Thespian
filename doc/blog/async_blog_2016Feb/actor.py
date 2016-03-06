from thespian.actors import *
from datetime import datetime, timedelta


class CountDown(Actor):
    def receiveMessage(self, msg, sender):
        if isinstance(msg, tuple):
            self.starter = sender
            self.label, self.length = msg[:2]
            self.delay = msg[2] if len(msg) == 3 else 0
            print(self.label, 'waiting', self.delay,
                  'seconds before starting countdown')
            if self.delay:
                self.wakeupAfter(timedelta(seconds=self.delay))
                return
        elif isinstance(msg, ActorExitRequest):
            return
        # msg is either WakeupMessage or fall-thru from tuple above if
        # delay is 0
        if self.delay is not None:
            print(self.label, 'starting after waiting',
                  msg.delayPeriod if isinstance(msg, WakeupMessage) else
                  self.delay)
            self.delay = None
        if self.length:
            print(self.label, 'T-minus', self.length)
            self.length -= 1
            self.wakeupAfter(timedelta(seconds=1))
        else:
            print(self.label, 'lift-off!')
            self.send(self.starter, self.myAddress)


def main(base):
    asys = ActorSystem(base)
    try:
        actors = [asys.createActor(CountDown) for _ in range(3)]
        start = datetime.now()
        asys.tell(actors[0], ('A', 5))
        asys.tell(actors[1], ('B', 3, 2))
        asys.tell(actors[2], ('C', 4, 1))
        while actors:
            rsp = asys.listen(timedelta(seconds=10))
            del actors[actors.index(rsp)]
        print('Total elapsed time is', datetime.now() - start)
    finally:
        asys.shutdown()


if __name__ == "__main__":
    import sys
    # Try:  python3 actor.py {BASENAME}
    #   where basename is one of: simpleSystemBase, multiprocUDPBase
    #                             multiprocTCPBase, multiprocQueueBase }
    main((sys.argv+['simpleSystemBase'])[1])
