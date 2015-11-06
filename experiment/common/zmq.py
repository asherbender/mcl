from __future__ import absolute_import

import zmq
import time
import Queue
import msgpack
import threading

from .common import print_if
from .common import get_utc_string
from .common import utc_str_to_datetime

BASE_PORT = 5550
LOCALHOST = False
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

        # Create message
        message = {'ping_PID': PID,
                   'counter': counter,
                   'payload': payload,
                   'ping_time': get_utc_string()}

        # Publish data.
        self.__socket.send(msgpack.dumps(message))

    def close(self):

        self.__socket.close()
        self.__context.term()


class SendPong(object):

    def __init__(self, PID, ID, broadcasters, verbose, max_chars):

        # Create event for terminating event loop.
        self.__run_event = threading.Event()
        self.__run_event.set()

        # Create thread for handling event loop.
        self.__event_loop = threading.Thread(target=self.__event_loop,
                                             args=(self.__run_event, PID, ID,
                                                   broadcasters,
                                                   verbose, max_chars))
        self.__event_loop.daemon = True
        self.__event_loop.start()

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
                socks = dict(poller.poll(timeout=1))
                if ping_socket in socks and socks[ping_socket] == zmq.POLLIN:
                    payload = ping_socket.recv()

                    ping = msgpack.loads(payload)
                    pong = {'ping_PID': ping['ping_PID'],
                            'counter': ping['counter'],
                            'pong_PID': PID,
                            'payload': ping['payload'],
                            'pong_time': get_utc_string()}

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
        self.__event_loop.join()


class LogPingPong(object):

    @property
    def pings(self):
        if isinstance(self.__pings, (list, )):
            return self.__pings
        else:
            return None

    @property
    def pongs(self):
        if isinstance(self.__pongs, (list, )):
            return self.__pongs
        else:
            return None

    def __init__(self, broadcasters, listeners):

        self.__pings = Queue.Queue()
        self.__pongs = Queue.Queue()

        # Create event for terminating event loop.
        self.__run_event = threading.Event()
        self.__run_event.set()

        # Create thread for logging pings and pongs.
        self.__event_loop = threading.Thread(target=self.__event_loop,
                                             args=(self.__run_event,
                                                   broadcasters,
                                                   listeners,
                                                   self.__pings,
                                                   self.__pongs))

        self.__event_loop.daemon = True
        self.__event_loop.start()

    @staticmethod
    def __event_loop(run_event, broadcasters, listeners, ping_queue, pong_queue):

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
                socks = dict(poller.poll(timeout=1))

                # Store pongs.
                if pong_socket in socks and socks[pong_socket] == zmq.POLLIN:
                    pong_queue.put(msgpack.loads(pong_socket.recv()))

                # Store pings.
                if ping_socket in socks and socks[ping_socket] == zmq.POLLIN:
                    ping_queue.put(msgpack.loads(ping_socket.recv()))

        except KeyboardInterrupt:
            pass

        ping_socket.close()
        pong_socket.close()
        context.term()

    def close(self):

        # Stop listening for data.
        self.__run_event.clear()
        self.__event_loop.join()
        self.__pings.put('END')
        self.__pongs.put('END')
        time.sleep(0.1)

        # Convert ping queue to a list (make stored format identical to other
        # transports). Drop payload.
        pings = list()
        for ping in iter(self.__pings.get, 'END'):
            pings.append({'ping_PID': int(ping['ping_PID']),
                          'counter': int(ping['counter']),
                          'ping_time': utc_str_to_datetime(ping['ping_time'])})

        # Convert pong queue to a list (make stored format identical to other
        # transports). Drop payload.
        pongs = list()
        for pong in iter(self.__pongs.get, 'END'):
            pongs.append({'ping_PID': int(pong['ping_PID']),
                          'counter': int(pong['counter']),
                          'pong_PID': int(pong['pong_PID']),
                          'pong_time': utc_str_to_datetime(pong['pong_time'])})

        # Store lists.
        self.__pings = sorted(pings, key=lambda ping: ping['counter'])
        self.__pongs = sorted(pongs, key=lambda pong: pong['counter'])
