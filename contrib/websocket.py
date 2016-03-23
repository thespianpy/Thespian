"""
A generic websocket client wrapped in an Actor

Requires thespian (of course) and websocket packages.

Linux-specific due to use of epoll(); can be changed to use select()
fairly easily, but performance will suffer

"""

from __future__ import absolute_import, division, print_function

import select
import logging as log
from datetime import timedelta
from collections import namedtuple

import websocket
from websocket import ABNF
from thespian.actors import ActorExitRequest, WakeupMessage, Actor

# Message to send to open the connection
Start_Websocket = namedtuple('Start_Websocket', 'ws_addr start_msg upstream')
# Message type that's sent to the 'upstream'
Websocket_Output = namedtuple('Websocket_Output', 'msg')
# Message to send to send more data out the websocket
Websocket_Input = namedtuple('Websocket_Input', 'msg')

# Maximum number of messages to read per wakeup ; raise this if you see
# a lot of "WebsocketClientActor not keeping up with incoming websocket data"
# messages in the log output
MAX_MSGS_PER_READ = 50

class WebsocketClientActor(Actor):
    """
    A websocket client wrapped in an Actor

    This was originally written to support fetching streaming data via
    a websocket; the Websocket_Input bits are less stress-tested.

    Usage:

        start_msg = "subscribe"
        ws_addr = "wss://ws-feed.somesite.com"
        startmsg = Start_Websocket(ws_addr, start_msg, receipient_Actor)
        self.client = self.createActor(WebsocketClientActor)
        self.send(self.client, startmsg)

    ...and recipient_Actor will start receiveing Websocket_Output messages

    """

    def __init__(self):
        super(WebsocketClientActor, self).__init__()

        self.started = False
        self.running = False
        self.ws = None


    def check_websocket(self):
        msgs = 0
        events = self.epoll.poll(0)
        while events and msgs < MAX_MSGS_PER_READ:
            for fileno, event in events:
                if not (event & select.EPOLLIN):
                    self.send(self.myAddress, ActorExitRequest())
                op_code, frame = self.ws.recv_data_frame(True)
                if op_code == ABNF.OPCODE_CLOSE:
                    self.send(self.myAddress, ActorExitRequest())
                elif op_code in (ABNF.OPCODE_PING, ABNF.OPCODE_PONG, ABNF.OPCODE_CONT):
                    pass # ignore
                else:
                    msgs += 1
                    self.send(self.config.upstream, Websocket_Output(frame.data))
            events = self.epoll.poll(0)
        if msgs >= MAX_MSGS_PER_READ:
            log.critical("WebsocketClientActor not keeping up with incoming websocket data")


    def receiveMsg_Start_Websocket(self, m, sender):
        if self.started: # already started
            return
        self.config = m
        self.started = True
        self.running = True

        # open the connection
        websocket.enableTrace(False)
        self.ws = websocket.create_connection(m.ws_addr)
        log.info("Websocket Connected")

        # set up the socket monitoring
        self.epoll = select.epoll()
        mask = select.EPOLLIN | select.EPOLLHUP | select.EPOLLERR
        self.epoll.register(self.ws.sock.fileno(), mask)

        # subscribe to the feed
        self.ws.send(m.start_msg)

        # start checking for data
        self.send(self.myAddress, WakeupMessage(None))


    def receiveMsg_Websocket_Input(self, m, sender):
        if not self.running: # can't send
            return
        log.debug("Websocket sending %r", m.msg)
        self.ws.send(m.msg)


    def receiveMsg_WakeupMessage(self, m, sender):
        if not self.running: # stopped
            return
        try:
            self.check_websocket()
        except Exception as e:
            log.error("Got exception: %r", e)
            self.send(self.myAddress, ActorExitRequest())
            raise

        self.wakeupAfter(timedelta(milliseconds=20))


    def receiveMsg_ActorExitRequest(self, m, sender):
        """Stop the Websocket, and the actor"""
        log.info("Websocket exiting")
        self.running = False
        self.epoll.close()
        self.ws.close()

    def receiveMessage(self, m, sender):
        handler = { WakeupMessage: self.receiveMsg_WakeupMessage,
                    Start_Websocket: self.receiveMsg_Start_Websocket,
                    Websocket_Input: self.receiveMsg_Websocket_Input,
                    ActorExitRequest: self.receiveMsg_ActorExitRequest
                   }.get(type(m), None)
        if handler is None:
            log.error("Unhandled message %r from %r", m, sender)
            return
        handler(m, sender)

