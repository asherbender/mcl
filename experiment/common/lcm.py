from __future__ import absolute_import

import lcm
import time
import select
import threading

from .common import print_if
from .lcm_msgs import PingMessage_t
from .lcm_msgs import PongMessage_t

PING_CHANNEL = 'LCM-ping'
PONG_CHANNEL = 'LCM-pong'
LCM_ADDRESS = 'udpm://224.0.0.1:7667?ttl=1'


class SendPing(object):

    @property
    def messages(self):
        return self.__messages

    def __init__(self, ID):

        self.__messages = list()
        self.__lc = lcm.LCM(LCM_ADDRESS)

    def publish(self, PID, counter, payload):

        ping = PingMessage_t()
        ping.ping_PID = PID
        ping.counter = counter
        ping.payload = payload
        ping.ping_time = time.time()

        # Publish message.
        self.__lc.publish(PING_CHANNEL, ping.encode())
        self.__messages.append(ping)

    def __ping_to_dct(self, msg):

        return {'ping_PID': int(msg.ping_PID),
                'counter': int(msg.counter),
                'payload': str(msg.payload),
                'ping_time': float(msg.ping_time)}

    def close(self):

        self.__messages = [self.__ping_to_dct(msg) for msg in self.__messages]


class SendPong(object):

    @property
    def messages(self):
        return self.__messages

    def __init__(self, PID, ID, broadcasters, verbose):

        self.__messages = list()
        self.__lc = lcm.LCM(LCM_ADDRESS)

        def callback(channel, data):
            """Repack PingMessage() data as a PongMessage()."""

            ping = PingMessage_t.decode(data)
            pong = PongMessage_t()
            pong.ping_PID = ping.ping_PID
            pong.counter = ping.counter
            pong.pong_PID = PID
            pong.payload = ping.payload
            pong.pong_time = time.time()

            self.__lc.publish(PONG_CHANNEL, pong.encode())
            self.__messages.append(pong)

            if verbose:
                s = 'PID %4i (LCM): sent pong message %i'
                s = s % (PID, pong.counter)
                print_if(verbose, s)

        self.__subscription = self.__lc.subscribe(PING_CHANNEL, callback)

        # Create event for terminating event loop.
        self.__run_event = threading.Event()
        self.__run_event.set()

        # Create thread for handling event loop.
        self.__thread = threading.Thread(target=self.__event_loop,
                                         args=(self.__run_event,
                                               self.__lc))
        self.__thread.daemon = True
        self.__thread.start()

    @staticmethod
    def __event_loop(run_event, lc):

        try:
            while run_event.is_set():
                rfds, wfds, efds = select.select([lc.fileno()], [], [], 0.1)
                if rfds:
                    lc.handle()

        except KeyboardInterrupt:
            pass

    def __pong_to_dct(self, msg):

        return {'ping_PID': int(msg.ping_PID),
                'counter': int(msg.counter),
                'pong_PID': int(msg.pong_PID),
                'payload': str(msg.payload),
                'pong_time': float(msg.pong_time)}

    def close(self):

        self.__run_event.clear()
        self.__thread.join()
        self.__lc.unsubscribe(self.__subscription)

        self.__messages = [self.__pong_to_dct(msg) for msg in self.__messages]


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

        self.__lc = lcm.LCM(LCM_ADDRESS)

        self.__pings = list()
        callback = lambda chl, d: self.__pings.append(PingMessage_t.decode(d))
        self.__ping_subscription = self.__lc.subscribe(PING_CHANNEL, callback)

        self.__pongs = list()
        callback = lambda chl, d: self.__pongs.append(PongMessage_t.decode(d))
        self.__pong_subscription = self.__lc.subscribe(PONG_CHANNEL, callback)

        # Create event for terminating event loop.
        self.__run_event = threading.Event()
        self.__run_event.set()

        # Create thread for handling event loop.
        self.__thread = threading.Thread(target=self.__event_loop,
                                         args=(self.__run_event,
                                               self.__lc))
        self.__thread.daemon = True
        self.__thread.start()

    @staticmethod
    def __event_loop(run_event, lc):

        try:
            while run_event.is_set():
                rfds, wfds, efds = select.select([lc.fileno()], [], [], 0.1)
                if rfds:
                    lc.handle()

        except KeyboardInterrupt:
            pass

    def close(self):

        # Stop listening for data.
        self.__run_event.clear()
        self.__thread.join()
        self.__lc.unsubscribe(self.__ping_subscription)
        self.__lc.unsubscribe(self.__pong_subscription)

        # Convert ping queue to a list (make stored format identical to other
        # transports). Drop payload.
        pings = list()
        for item in self.__pings:
            pings.append({'ping_PID': int(item.ping_PID),
                          'counter': int(item.counter),
                          'ping_time': float(item.ping_time)})

        # Convert pong queue to a list (make stored format identical to other
        # transports). Drop payload.
        pongs = list()
        for item in self.__pongs:
            pongs.append({'ping_PID': int(item.ping_PID),
                          'counter': int(item.counter),
                          'pong_PID': int(item.pong_PID),
                          'pong_time': float(item.pong_time)})

        # Store lists.
        self.__pings = sorted(pings, key=lambda ping: ping['counter'])
        self.__pongs = sorted(pongs, key=lambda pong: pong['counter'])
