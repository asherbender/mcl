from __future__ import absolute_import

import time
import msgpack
from mcl.network.udp import Connection
from mcl.network.udp import RawListener
from mcl.network.udp import RawBroadcaster

from .common import print_if
from .common import get_utc_string
from .common import format_ping_pongs

ping_URL = 'ff15::c75d:ce41:ea8e:000a'
pong_URL = 'ff15::c75d:ce41:ea8e:000b'


class SendPing(object):

    def __init__(self, ID):

        self.__broadcaster = RawBroadcaster(Connection(ping_URL))

    def publish(self, PID, counter, payload):

        # Create message
        message = {'ping_PID': PID,
                   'counter': counter,
                   'payload': payload,
                   'ping_time': get_utc_string()}

        # Publish message.
        self.__broadcaster.publish(msgpack.dumps(message))

    def close(self):

        self.__broadcaster.close()


class SendPong(object):

    def __init__(self, PID, ID, broadcasters, verbose, max_chars):

        # Create message listener and broadcaster for PingMessage() and
        # PongMessage().
        self.__listener = RawListener(Connection(ping_URL))
        self.__broadcaster = RawBroadcaster(Connection(pong_URL))

        def callback(data):
            """Repack PingMessage() data as a PongMessage()."""

            ping = msgpack.loads(data['payload'])
            pong = {'ping_PID': ping['ping_PID'],
                    'counter': ping['counter'],
                    'pong_PID': PID,
                    'payload': ping['payload'],
                    'pong_time': get_utc_string()}

            self.__broadcaster.publish(msgpack.dumps(pong))

            if verbose:
                s = 'PID %4i (mcl): sent pong message %i'
                s = s % (PID, pong['counter'])
                print_if(verbose, s, max_chars)

        self.__listener.subscribe(callback)

    def close(self):

        self.__listener.close()
        self.__broadcaster.close()


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

        self.__pings = list()
        self.__pongs = list()
        self.__ping_listener = RawListener(Connection(ping_URL))
        self.__pong_listener = RawListener(Connection(pong_URL))

        self.__ping_listener.subscribe(lambda msg: self.__pings.append(msgpack.loads(msg['payload'])))
        self.__pong_listener.subscribe(lambda msg: self.__pongs.append(msgpack.loads(msg['payload'])))

    def close(self):

        # Stop listening for data.
        self.__ping_listener.close()
        self.__pong_listener.close()
        time.sleep(0.1)

        # Ensure ping/pongs are stored in an identical format. Drop the payload
        # to save space.
        self.__pings, self.__pongs = format_ping_pongs(self.__pings,
                                                       self.__pongs)
