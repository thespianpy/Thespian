"""Initial http server.

  Uses the ThespianWatch functionality to manage a set of sockets: one
  listening socket and a number of connected, receiving sockets.  This
  implementation is better than the http_server1 example in that this
  version provides reasonable responsiveness due to "event-loop" style
  functionality of the Server Actor.  However, ThespianWatch
  functionality is not available with all Actor System bases.

  All sockets are managed by the main server actor.  When a complete
  HTTP request has been received, it is sent to the Handler actor
  (provided at startup); the Handler actor sends back a response which
  is then sent to the client.

                 +---------+     +---------+
   Clients------>| Server  |---->| Handler |
                 +---------+     +---------+

  Limitations:

  * Because the socket is in a different Actor than the handler, the
    handler cannot easily read incoming post data or stream large
    output data.

  * Only HTTP GET is supported, and the response is sent in a single
    buffer (no streaming).

  * Completely ignores all input, simply responds to all queries with
    a Hello World response.

  Run this example via:
    $ python examples/httpserver/http_server2.py 8000

  View sample output in another window via:
    $ curl http://localhost:8000/

"""

import sys
from thespian.actors import *
import logging
import select
import socket
import errno
from datetime import timedelta
from functools import partial
from common import *


class Handler(ActorTypeDispatcher):
    def receiveMsg_HTTPRequest(self, reqmsg, sender):
        # reqmsg.environ
        self.send(sender, HTTPResponse(reqmsg, '<h1>Hello, World!</h1>'))


class StartServer(object):
    """Message sent to ServerActor to start HTTP server on a give port,
       passing requests to the handler actor"""
    def __init__(self, port, handler):
        self.port = port
        self.handler = handler


class ServerActor(ActorTypeDispatcher):
    def __init__(self, *args, **kw):
        super(ServerActor, self).__init__(*args, **kw)
        self._response_only = []

    def check_socket(self, incsock):
        """Reads incoming data from a client-connected socket.  When a full
           HTTP request has been received, sends it to the handler
        """
        try:
            newdata = incsock.socket.recv(incsock.incbuf.remaining())
        except IOError as ex:
            if ex.errno == errno.EAGAIN:
                return True  # nothing to read right now
            raise
        if not newdata:
            # The other side may have fully closed, or may have just
            # done a shutdown WR.
            if incsock.responded:
                incsock.close()
                logging.info('Client socket from %s: closed', incsock.rmtaddr)
                return False
            else:
                self._response_only.append(incsock)
        logging.info('Client socket from %s: newdata[%d] -> %s', incsock.rmtaddr, len(newdata), newdata[:100])
        incsock.incbuf.addMore(newdata)
        while incsock.incbuf.isComplete():
            request, incsock.incbuf = incsock.incbuf.extract(partial(HTTPRequest, self.serveAddress, incsock.rmtaddr))
            self.send(self.handler, request)
        return True


    def _watchlist(self):
        if hasattr(self, 'servesocket'):
            return ThespianWatch([self.servesocket.fileno()] +
                                 [S.socket.fileno() for S in self.activesockets])
        return None


    def receiveMsg_HTTPResponse(self, msg, sender):
        """Got an HTTP response from the handler; find the socket
           corresponding to the request and send the response"""
        for each in self.activesockets:
            if each.rmtaddr == msg.request.rmtaddr:
                each.socket.sendall(msg.serialize())
                each.responded = True
                break
        else:
            for each in self._response_only:
                if each.rmtaddr == msg.request.rmtaddr:
                    print('Sending and closing:',str(msg.serialize()))
                    each.socket.sendall(msg.serialize())
                    self._response_only = [ RO for RO in self._response_only
                                            if RO != each ]
                    each.close()
                    break
            else:
                logging.warning('Got HTTP response for %s but no socket to send', msg.request.rmtaddr)
        return self._watchlist()


    def check_incoming(self):
        "Check the main listening socket for new connections"
        logging.info('Check Incoming')
        try:
            while True:
                newsock = self.servesocket.accept()
                if not newsock: break
                logging.info('Connect from %s', newsock[1])
                self.activesockets.append(IncomingSocket(newsock))
        except IOError:
            pass


    def receiveMsg_StartServer(self, m, sender):
        """Creates the listening socket on the specified port and starts the
           scheduled wakeups
        """
        logging.info('Starting server on %s', m.port)
        self.handler = m.handler
        self.serveAddress = ('', m.port)
        if not getattr(self, 'servesocket', None):
            self.servesocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
            try:
                self.servesocket.bind(self.serveAddress)
                self.servesocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.servesocket.setblocking(0)
                self.servesocket.listen(5)
            except Exception:
                delattr(self, 'servesocket')
            self.activesockets = []
        return self._watchlist()


    def receiveMsg_WatchMessage(self, m, sender):
        "Checks for more stuff to handle on the sockets"
        if not hasattr(self, 'servesocket'):
            return  # shutdown
        if self.servesocket.fileno() in m.ready:
            self.check_incoming()
        self.activesockets = [A
                              for A in self.activesockets
                              if A.socket.fileno() not in m.ready or self.check_socket(A)]
        return self._watchlist()


    def receiveMsg_ActorExitRequest(self, m, sender):
        "Shut everyting down; actor is exiting"
        if hasattr(self, 'servesocket'):
            self.servesocket.close()
            for each in self.activesockets:
                each.socket.close()


try:
    import msvcrt as m
    def wait_for_user():
        print('Hit any key to continue')
        m.getch()
except Exception:
    import signal
    def wait_for_user():
        print('Hit Ctrl-C to exit')
        try:
            signal.pause()
        except KeyboardInterrupt:
            pass


def main(portnum):
    asys = ActorSystem('multiprocTCPBase')
    try:
        server = asys.createActor(ServerActor)
        handler = asys.createActor(Handler)
        asys.tell(server, StartServer(portnum, handler))
        wait_for_user()
        print('Shutting down')
        asys.tell(server, ActorExitRequest())
        asys.tell(handler, ActorExitRequest())
    finally:
        asys.shutdown()



if __name__ == '__main__':
    main(int((sys.argv + [8080])[1]))
