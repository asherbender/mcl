import time
import Queue
import mcl.message.messages
from common import print_if
from common import get_utc_string
from mcl.network.udp import Connection
from mcl.network.network import MessageListener
from mcl.network.network import MessageBroadcaster


ping_URL = 'ff15::c75d:ce41:ea8e:000a'
pong_URL = 'ff15::c75d:ce41:ea8e:000b'


class PingMessage(mcl.message.messages.Message):
    mandatory = ('ping_PID', 'counter', 'payload', 'ping_time')
    connection = Connection(ping_URL)


class PongMessage(mcl.message.messages.Message):
    mandatory = ('ping_PID', 'counter',
                 'pong_PID', 'payload', 'pong_time')
    connection = Connection(pong_URL)


class SendPing(object):

    def __init__(self):

        self.__broadcaster = MessageBroadcaster(PingMessage)

    def publish(self, PID, counter, payload):

        ping = PingMessage(ping_PID=PID,
                           counter=counter,
                           payload=payload,
                           ping_time=get_utc_string())

        # Publish message.
        self.__broadcaster.publish(ping)

    def close(self):

        self.__broadcaster.close()


class SendPong(object):

    def __init__(self, PID, verbose, max_chars):

        # Create message listener and broadcaster for PingMessage() and
        # PongMessage().
        self.__listener = MessageListener(PingMessage)
        self.__broadcaster = MessageBroadcaster(PongMessage)

        def callback(data):
            """Repack PingMessage() data as a PongMessage()."""

            pong = PongMessage(ping_PID=data['ping_PID'],
                               counter=data['counter'],
                               pong_PID=PID,
                               payload=data['payload'],
                               pong_time=get_utc_string())

            self.__broadcaster.publish(pong)

            if verbose:
                s = 'PID %4i (MCL): sent pong message %i'
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

    def __init__(self):

        self.__pings = Queue.Queue()
        self.__pongs = Queue.Queue()
        self.__ping_listener = MessageListener(PingMessage)
        self.__pong_listener = MessageListener(PongMessage)

        self.__ping_listener.subscribe(lambda msg: self.__pings.put(msg))
        self.__pong_listener.subscribe(lambda msg: self.__pongs.put(msg))

    def close(self):

        # Stop listening for data.
        self.__ping_listener.close()
        self.__pong_listener.close()
        self.__pings.put('END')
        self.__pongs.put('END')
        time.sleep(0.1)

        # Convert ping queue to a list (make stored format similar to other
        # transports).
        pings = list()
        for ping in iter(self.__pings.get, 'END'):
            ping = dict(ping)
            del(ping['timestamp'])
            del(ping['name'])
            pings.append(ping)

        # Convert pong queue to a list (make stored format similar to other
        # transports).
        pongs = list()
        for pong in iter(self.__pongs.get, 'END'):
            pong = dict(pong)
            del(pong['timestamp'])
            del(pong['name'])
            pongs.append(pong)

        self.__pings = pings
        self.__pongs = pongs
