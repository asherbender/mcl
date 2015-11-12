from __future__ import absolute_import

import zmq
import Queue
import msgpack
import threading

from .common import LOCALHOST
from .common import print_if
from .common import create_ping
from .common import create_pong
from .common import format_ping_pongs

BASE_PORT = 5550
if LOCALHOST:
    ZMQ_PING = 'tcp://127.0.0.1'
    ZMQ_PONG = 'tcp://127.0.0.2'
else:
    ZMQ_PING = 'tcp://10.0.0.100'
    ZMQ_PONG = 'tcp://10.0.0.'


def create_ping_address(ip, port, ID):

    return '%s:%i' % (ip, port + ID)


def create_pong_address(ip, port, ID):

    if LOCALHOST:
        return '%s:%i' % (ip, port + ID)
    else:
        return '%s%i:%i' % (ip, ID, port + ID)


class SendPing(object):

    def __init__(self, ID):

        # Create socket.
        self.__context = zmq.Context()
        self.__socket = self.__context.socket(zmq.PUB)
        self.__socket.bind(create_ping_address(ZMQ_PING, BASE_PORT, ID))

    def publish(self, PID, counter, payload):

        # Publish 'ping' message.
        ping = create_ping(PID, counter, payload)
        self.__socket.send(msgpack.dumps(ping))

    def close(self):

        self.__socket.close()
        self.__context.term()


class SendPong(object):

    def __init__(self, PID, ID, broadcasters, verbose, max_chars):

        # Create event for terminating event loop.
        self.__run_event = threading.Event()
        self.__run_event.set()

        # Create thread for handling event loop.
        self.__thread = threading.Thread(target=self.__event_loop,
                                         args=(self.__run_event, PID, ID,
                                               broadcasters,
                                               verbose, max_chars))
        self.__thread.daemon = True
        self.__thread.start()

    @staticmethod
    def __event_loop(run_event, PID, ID, broadcasters, verbose, max_chars):

        # Create socket for receiving ping messages.
        context = zmq.Context()
        ping_socket = context.socket(zmq.SUB)
        ping_socket.setsockopt(zmq.SUBSCRIBE, '')

        # Connect socket to all 'ping' publishers.
        for i in range(0, broadcasters):
            ping_socket.connect(create_ping_address(ZMQ_PING, BASE_PORT, i))

        # Create socket for sending pong messages.
        pong_socket = context.socket(zmq.PUB)
        pong_socket.bind(create_pong_address(ZMQ_PONG, BASE_PORT, ID))

        # Create object for retrieving messages.
        poller = zmq.Poller()
        poller.register(ping_socket, zmq.POLLIN)
        try:
            while run_event.is_set():
                socks = dict(poller.poll(timeout=100))
                if ping_socket in socks and socks[ping_socket] == zmq.POLLIN:
                    payload = ping_socket.recv()

                    # Publish 'pong' message.
                    ping = msgpack.loads(payload)
                    pong = create_pong(PID, ping)
                    pong_socket.send(msgpack.dumps(pong))

                    if verbose:
                        s = 'PID %4i (ZMQ): sent pong message %i'
                        s = s % (PID, pong['counter'])
                        print_if(verbose, s, max_chars)

        except KeyboardInterrupt:
            pass

        ping_socket.close()
        pong_socket.close()
        context.term()

    def close(self):

        self.__run_event.clear()
        self.__thread.join()


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
        self.__thread = threading.Thread(target=self.__event_loop,
                                         args=(self.__run_event,
                                               broadcasters,
                                               listeners,
                                               self.__queue))

        self.__thread.daemon = True
        self.__thread.start()

    @staticmethod
    def __event_loop(run_event, broadcasters, listeners, queue):

        # Create local lists for storing messages.
        ping_list = list()
        pong_list = list()

        # Create sockets for receiving ping and pong messages.
        context = zmq.Context()
        ping_socket = context.socket(zmq.SUB)
        pong_socket = context.socket(zmq.SUB)
        ping_socket.setsockopt(zmq.SUBSCRIBE, '')
        pong_socket.setsockopt(zmq.SUBSCRIBE, '')

        # Connect socket to all 'ping' publishers.
        for i in range(0, broadcasters):
            ping_socket.connect(create_ping_address(ZMQ_PING, BASE_PORT, i))

        # Connect socket to all 'pong' publishers.
        for i in range(0, listeners):
            if LOCALHOST:
                pong_socket.connect(create_pong_address(ZMQ_PONG, BASE_PORT, i))
            else:
                pong_socket.connect(create_pong_address(ZMQ_PONG, BASE_PORT, i + 1))

        # Create object for retrieving messages.
        poller = zmq.Poller()
        poller.register(ping_socket, zmq.POLLIN)
        poller.register(pong_socket, zmq.POLLIN)

        try:
            while run_event.is_set():
                socks = dict(poller.poll(timeout=100))

                # Store pongs.
                if pong_socket in socks and socks[pong_socket] == zmq.POLLIN:
                    pong_list.append(msgpack.loads(pong_socket.recv()))

                # Store pings.
                if ping_socket in socks and socks[ping_socket] == zmq.POLLIN:
                    ping_list.append(msgpack.loads(ping_socket.recv()))

        except KeyboardInterrupt:
            pass

        # Close connection.
        ping_socket.close()
        pong_socket.close()
        context.term()

        # Communicate messages.
        queue.put(ping_list)
        queue.put(pong_list)

    def close(self):

        # Stop listening for data.
        self.__run_event.clear()
        self.__thread.join()
        self.__pings = self.__queue.get()
        self.__pongs = self.__queue.get()

        # Ensure ping/pongs are stored in an identical format. Drop the payload
        # to save space.
        self.__pings, self.__pongs = format_ping_pongs(self.__pings,
                                                       self.__pongs)
