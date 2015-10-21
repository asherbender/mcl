import lcm
import time
import Queue
import select
import threading
from common import print_if
from common import get_utc_string
from lcm_msgs import PingMessage_t
from lcm_msgs import PongMessage_t


PING_CHANNEL = 'LCM-ping'
PONG_CHANNEL = 'LCM-pong'


class SendPing(object):

    def __init__(self):
        self.__lc = lcm.LCM()

    def publish(self, PID, counter, payload):

        ping = PingMessage_t()
        ping.ping_PID = PID
        ping.counter = counter
        ping.payload = payload
        ping.ping_time = get_utc_string()

        # Publish message.
        self.__lc.publish(PING_CHANNEL, ping.encode())

    def close(self):
        pass


class SendPong(object):

    def __init__(self, PID, verbose, max_chars):

        self.__lc = lcm.LCM()

        def callback(channel, data):
            """Repack PingMessage() data as a PongMessage()."""

            ping = PingMessage_t.decode(data)
            pong = PongMessage_t()
            pong.ping_PID = ping.ping_PID
            pong.counter = ping.counter
            pong.pong_PID = PID
            pong.payload = ping.payload
            pong.pong_time = get_utc_string()

            self.__lc.publish(PONG_CHANNEL, pong.encode())

            if verbose:
                s = 'PID %4i (LCM): sent pong message %i'
                s = s % (PID, pong.counter)
                print_if(verbose, s, max_chars)

        self.__subscription = self.__lc.subscribe(PING_CHANNEL, callback)

        # Create event for terminating event loop.
        self.__run_event = threading.Event()
        self.__run_event.set()

        # Create thread for handling event loop.
        self.__event_loop = threading.Thread(target=self.__event_loop,
                                             args=(self.__run_event,
                                                   self.__lc))
        self.__event_loop.daemon = True
        self.__event_loop.start()

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

        self.__run_event.clear()
        self.__event_loop.join()
        self.__lc.unsubscribe(self.__subscription)


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

    def __init__(self):

        self.__lc = lcm.LCM()

        self.__pings = Queue.Queue()
        self.__pongs = Queue.Queue()
        ping_callback = lambda chl, data: self.__pings.put(PingMessage_t.decode(data))
        pong_callback = lambda chl, data: self.__pongs.put(PongMessage_t.decode(data))
        self.__pingsub = self.__lc.subscribe(PING_CHANNEL, ping_callback)
        self.__pongsub = self.__lc.subscribe(PONG_CHANNEL, pong_callback)

        # Create event for terminating event loop.
        self.__run_event = threading.Event()
        self.__run_event.set()

        # Create thread for handling event loop.
        self.__event_loop = threading.Thread(target=self.__event_loop,
                                             args=(self.__run_event,
                                                   self.__lc))
        self.__event_loop.daemon = True
        self.__event_loop.start()

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
        self.__event_loop.join()
        self.__lc.unsubscribe(self.__pingsub)
        self.__lc.unsubscribe(self.__pongsub)
        self.__pings.put('END')
        self.__pongs.put('END')
        time.sleep(0.1)

        # Convert ping queue to a list.
        pings = list()
        for item in iter(self.__pings.get, 'END'):
            pings.append({'ping_PID': int(item.ping_PID),
                          'counter': int(item.counter),
                          'payload': str(item.payload),
                          'ping_time': str(item.ping_time)})

        # Convert pong queue to a list.
        pongs = list()
        for item in iter(self.__pongs.get, 'END'):
            pongs.append({'ping_PID': int(item.ping_PID),
                          'counter': int(item.counter),
                          'pong_PID': int(item.pong_PID),
                          'payload': str(item.payload),
                          'pong_time': str(item.pong_time)})

        # Store lists.
        self.__pings = pings
        self.__pongs = pongs
