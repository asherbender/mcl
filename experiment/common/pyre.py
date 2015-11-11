from __future__ import absolute_import

import zmq
import pyre
import Queue
import msgpack
import threading

from .common import print_if
from .common import get_utc_string
from .common import utc_str_to_datetime
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

        # Create message
        message = {'ping_PID': PID,
                   'counter': counter,
                   'payload': payload,
                   'ping_time': get_utc_string()}

        # Publish data.
        self.__node.shout(PING_GROUP, msgpack.dumps(message))

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

                        ping = msgpack.loads(payload[-1])
                        pong = {'ping_PID': ping['ping_PID'],
                                'counter': ping['counter'],
                                'pong_PID': PID,
                                'payload': ping['payload'],
                                'pong_time': get_utc_string()}

                        pong_node.shout(PONG_GROUPmsgpack.dumps(pong))

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

        # Convert ping queue to a list (make stored format identical to other
        # transports). Drop payload.
        pings = list()
        for ping in self.__pings:
            pings.append({'ping_PID': int(ping['ping_PID']),
                          'counter': int(ping['counter']),
                          'ping_time': utc_str_to_datetime(ping['ping_time'])})

        # Convert pong queue to a list (make stored format identical to other
        # transports). Drop payload.
        pongs = list()
        for pong in self.__pongs:
            pongs.append({'ping_PID': int(pong['ping_PID']),
                          'counter': int(pong['counter']),
                          'pong_PID': int(pong['pong_PID']),
                          'pong_time': utc_str_to_datetime(pong['pong_time'])})

        # Store lists.
        self.__pings = sorted(pings, key=lambda ping: ping['counter'])
        self.__pongs = sorted(pongs, key=lambda pong: pong['counter'])
