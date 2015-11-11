from __future__ import absolute_import

import zmq
import pyre
import Queue
import msgpack
import threading

from .common import print_if
from .common import create_ping
from .common import create_pong
from .common import format_ping_pongs

PING_GROUP = 'ping'
PONG_GROUP = 'pong'


class SendPing(object):

    def __init__(self, ID):

        # Creates a new Zyre node and join 'ping' group.
        self.__node = pyre.Pyre()
        self.__node.join(PING_GROUP)
        self.__node.start()

    def publish(self, PID, counter, payload):

        # Publish 'ping' message.
        ping = create_ping(PID, counter, payload)
        self.__node.shout(PING_GROUP, msgpack.dumps(ping))

    def close(self):

        # Signal peers that this node will go away.
        self.__node.stop()


class SendPong(object):

    def __init__(self, PID, ID, broadcasters, verbose, max_chars):

        # Create event for terminating event loop.
        self.__run_event = threading.Event()
        self.__run_event.set()

        # Create thread for handling event loop.
        self.__event_loop = threading.Thread(target=self.__event_loop,
                                             args=(self.__run_event, PID,
                                                   verbose, max_chars))
        self.__event_loop.daemon = True
        self.__event_loop.start()

    @staticmethod
    def __event_loop(run_event, PID, verbose, max_chars):

        # Creates a new Zyre node and join 'ping' group.
        ping_node = pyre.Pyre()
        ping_node.join(PING_GROUP)
        ping_node.start()

        # Creates a new Zyre node and join 'pong' group.
        pong_node = pyre.Pyre()
        pong_node.join(PONG_GROUP)
        pong_node.start()

        poller = zmq.Poller()
        poller.register(ping_node.inbox, zmq.POLLIN)
        while run_event.is_set():
            try:
                items = dict(poller.poll())
                if ((ping_node.inbox in items) and
                    (items[ping_node.inbox] == zmq.POLLIN)):
                    payload = ping_node.recv()
                    if payload[0] == 'SHOUT':

                        # Publish 'pong' message.
                        ping = msgpack.loads(payload[-1])
                        pong = create_pong(PID, ping)
                        pong_node.shout(PONG_GROUP, msgpack.dumps(pong))

                        if verbose:
                            s = 'PID %4i (pyre): sent pong message %i'
                            s = s % (PID, pong['counter'])
                            print_if(verbose, s, max_chars)

            except KeyboardInterrupt:
                break

        ping_node.stop()
        pong_node.stop()

    def close(self):

        self.__run_event.clear()
        self.__event_loop.join()


class LogPingPong(object):

    @property
    def pings(self):
        if self.__pings:
            return self.__pings
        else:
            return None

    @property
    def pongs(self):
        if self.__pongs:
            return self.__pongs
        else:
            return None

    def __init__(self, broadcasters, listeners):

        # Create objects for storing messages.
        self.__pings = None
        self.__pongs = None
        self.__queue = Queue.Queue()

        # Create event for terminating event loop.
        self.__run_event = threading.Event()
        self.__run_event.set()

        # Create thread for logging pings and pongs.
        self.__event_loop = threading.Thread(target=self.__event_loop,
                                             args=(self.__run_event,
                                                   self.__queue))

        self.__event_loop.daemon = True
        self.__event_loop.start()

    @staticmethod
    def __event_loop(run_event, queue):

        # Create local lists for storing messages.
        ping_list = list()
        pong_list = list()

        # Creates a new Zyre node and join 'ping' group.
        ping_node = pyre.Pyre()
        ping_node.join(PING_GROUP)
        ping_node.start()

        # Creates a new Zyre node and join 'pong' group.
        pong_node = pyre.Pyre()
        pong_node.join(PONG_GROUP)
        pong_node.start()

        poller = zmq.Poller()
        poller.register(ping_node.inbox, zmq.POLLIN)
        poller.register(pong_node.inbox, zmq.POLLIN)
        while run_event.is_set():
            try:
                items = dict(poller.poll())

                # Record ping messages.
                if ((pong_node.inbox in items) and
                    (items[pong_node.inbox] == zmq.POLLIN)):
                    payload = pong_node.recv()
                    if payload[0] == 'SHOUT':
                        ping_list.append(msgpack.loads(payload[-1]))

                # Record pong messages.
                elif ((ping_node.inbox in items) and
                      (items[ping_node.inbox] == zmq.POLLIN)):
                    payload = ping_node.recv()
                    if payload[0] == 'SHOUT':
                        pong_list.append(msgpack.loads(payload[-1]))

            except KeyboardInterrupt:
                break

        # Close connections.
        ping_node.stop()
        pong_node.stop()

        # Communicate messages.
        queue.put(ping_list)
        queue.put(pong_list)

    def close(self):

        # Stop listening for data.
        self.__run_event.clear()
        self.__event_loop.join()
        self.__pings = self.__queue.get()
        self.__pongs = self.__queue.get()

        # Ensure ping/pongs are stored in an identical format. Drop the payload
        # to save space.
        self.__pings, self.__pongs = format_ping_pongs(self.__pings,
                                                       self.__pongs)
