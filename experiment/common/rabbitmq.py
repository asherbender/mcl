from __future__ import absolute_import

import time
import msgpack
import threading
import multiprocessing
from amqpstorm import Connection

from .common import print_if
from .common import get_utc_string
from .common import utc_str_to_datetime

PORT = 5672
LOCALHOST = True
if LOCALHOST:
    HOSTNAME = 'localhost'
else:
    HOSTNAME = '10.0.0.101'
PING_EXCHANGE = 'ping-pong'
PONG_EXCHANGE = 'ping-pong'

USERNAME = 'test'
PASSWORD = 'test'


class Publisher(object):

    def __init__(self, host, username, password, exchange_name):
        self.channel = None
        self.connection = None
        self.host = host
        self.username = username
        self.password = password
        self.__exchange_name = exchange_name
        self.connect()

    def connect(self):

        self.connection = Connection(self.host, self.username, self.password)
        self.channel = self.connection.channel()
        self.channel.exchange.declare(self.__exchange_name,
                                      exchange_type='topic',
                                      auto_delete=True)

    def publish(self, message, topic=''):

        try:
            self.channel.basic.publish(message, topic,
                                       exchange=self.__exchange_name,
                                       mandatory=False,
                                       immediate=False)
        except:
            pass

    def close(self):
        self.channel.close()
        self.connection.close()


class Listener(object):

    def __init__(self, host, username, password, exchange_name, callback,
                 topic=''):

        self.__callback = callback

        # Create connection.
        self.__connection = Connection(host, username, password)

        # Create channel and declare exchange.
        self.__channel = self.__connection.channel()
        self.__channel.basic.qos(prefetch_count=65535)
        self.__channel.exchange.declare(exchange_name,
                                        exchange_type='topic',
                                        auto_delete=True)

        # Create queue and bind to exchange.
        queue = self.__channel.queue.declare(exclusive=True, auto_delete=True)
        self.__queue = queue['queue']
        self.__channel.queue.bind(exchange=exchange_name,
                                  queue=self.__queue,
                                  routing_key=topic)

        # Assign callback for consuming messages.
        self.__channel.basic.consume(self.__on_message,
                                     queue=self.__queue,
                                     no_ack=False)

        # Start processing messages.
        self.__thread = threading.Thread(target=self.__start_IO_loop)
        self.__thread.daemon = True
        self.__thread.start()

    def __start_IO_loop(self):

        try:
            self.__channel.start_consuming()
        except:
            pass

    def __on_message(self, body, channel, header, properties):

        self.__callback(body)
        channel.basic.ack(delivery_tag=header['delivery_tag'])

    def close(self):
        self.__connection.close()
        self.__thread.join()


class SendPing(object):

    def __init__(self, ID):

        self.__publisher = Publisher(HOSTNAME,
                                     USERNAME,
                                     PASSWORD,
                                     PING_EXCHANGE)

    def publish(self, PID, counter, payload):

        # Create message
        message = {'ping_PID': PID,
                   'counter': counter,
                   'payload': payload,
                   'ping_time': get_utc_string()}

        self.__publisher.publish(msgpack.dumps(message), 'ping')

    def close(self):
        self.__publisher.close()


class SendPong(object):

    def __init__(self, PID, ID, broadcasters, verbose, max_chars):

        self.__publisher = Publisher(HOSTNAME,
                                     USERNAME,
                                     PASSWORD,
                                     PONG_EXCHANGE)

        def callback(payload):

            ping = msgpack.loads(payload)
            pong = {'ping_PID': ping['ping_PID'],
                    'counter': ping['counter'],
                    'pong_PID': PID,
                    'payload': ping['payload'],
                    'pong_time': get_utc_string()}

            self.__publisher.publish(msgpack.dumps(pong), 'pong')

            if verbose:
                s = 'PID %4i (rabbitmq): sent pong message %i'
                s = s % (PID, pong['counter'])
                print_if(verbose, s, max_chars)

        self.__listener = Listener(HOSTNAME,
                                   USERNAME,
                                   PASSWORD,
                                   PING_EXCHANGE,
                                   callback, 'ping')

    def close(self):
        self.__publisher.close()
        self.__listener.close()


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

        self.__run_event = multiprocessing.Event()
        self.__pings = multiprocessing.Queue()
        self.__pongs = multiprocessing.Queue()
        self.__run_event.set()

        # Listen for pings.
        self.__ping_listener = multiprocessing.Process(target=self.__listen,
                                                       args=(PING_EXCHANGE,
                                                             self.__pings,
                                                             self.__run_event,
                                                             'ping'))
        self.__ping_listener.daemon = True
        self.__ping_listener.start()

        # Listen for pongs.
        self.__pong_listener = multiprocessing.Process(target=self.__listen,
                                                       args=(PONG_EXCHANGE,
                                                             self.__pongs,
                                                             self.__run_event,
                                                             'pong'))
        self.__pong_listener.daemon = True
        self.__pong_listener.start()

    @staticmethod
    def __listen(exchange_name, queue, run_event, topic):

        messages = list()
        listener = Listener(HOSTNAME,
                            USERNAME,
                            PASSWORD,
                            exchange_name,
                            lambda msg: messages.append(msg), topic)

        while run_event.is_set():
            time.sleep(0.1)

        listener.close()
        queue.put(messages)

    def close(self):

        # Stop listening for data.
        self.__run_event.clear()
        # self.__ping_listener.join()
        # self.__pong_listener.join()
        self.__pings = self.__pings.get()
        self.__pongs = self.__pongs.get()

        # Convert ping queue to a list (make stored format identical to other
        # transports). Drop payload.
        pings = list()
        for ping in self.__pings:
            ping = msgpack.loads(ping)
            pings.append({'ping_PID': int(ping['ping_PID']),
                          'counter': int(ping['counter']),
                          'ping_time': utc_str_to_datetime(ping['ping_time'])})

        # Convert pong queue to a list (make stored format identical to other
        # transports). Drop payload.
        pongs = list()
        for pong in self.__pongs:
            pong = msgpack.loads(pong)
            pongs.append({'ping_PID': int(pong['ping_PID']),
                          'counter': int(pong['counter']),
                          'pong_PID': int(pong['pong_PID']),
                          'pong_time': utc_str_to_datetime(pong['pong_time'])})

        # Store lists.
        self.__pings = sorted(pings, key=lambda ping: ping['counter'])
        self.__pongs = sorted(pongs, key=lambda pong: pong['counter'])
