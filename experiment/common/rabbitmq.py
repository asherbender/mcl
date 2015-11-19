from __future__ import absolute_import

import pika
import time
import Queue
import msgpack
import threading
import multiprocessing

from .common import print_if
from .common import create_ping
from .common import create_pong
from .common import format_ping_pongs

PORT = 5672
LOCALHOST = True
if LOCALHOST:
    HOSTNAME = 'localhost'
else:
    HOSTNAME = '10.0.0.100'

PING_TOPIC = 'ping'
PONG_TOPIC = 'pong'
PING_EXCHANGE = 'ping-pong'
PONG_EXCHANGE = 'ping-pong'

USERNAME = 'test'
PASSWORD = 'test'


def create_channel(exchange_name):

        # Create connection parameters.
        credentials = pika.PlainCredentials(USERNAME, PASSWORD)
        parameters = pika.ConnectionParameters(host=HOSTNAME,
                                               port=PORT,
                                               credentials=credentials)

        # Create connection.
        connection = pika.BlockingConnection(parameters)

        # Establish channel.
        channel = connection.channel()
        channel.exchange_declare(exchange=exchange_name,
                                 exchange_type='topic')

        return connection, channel


def create_queue(channel, exchange_name, routing_key=''):

    # Create queue for ping messages.
    result = channel.queue_declare(exclusive=True, auto_delete=True)
    queue_name = result.method.queue

    # Bind queue to exchange.
    channel.queue_bind(exchange=exchange_name, queue=queue_name,
                       routing_key=routing_key)

    return queue_name


def channel_get(channel, queue_name):

    method, header, payload = channel.basic_get(queue=queue_name)

    if method:
        channel.basic_ack(delivery_tag=method.delivery_tag, multiple=True)
        return payload
    else:
        return None


def empty_channel(channel, queue_name):

    while True:
        try:
            for meth, props, data in channel.consume(queue_name,
                                                     inactivity_timeout=1):
                if data:
                    channel.basic_ack(delivery_tag=meth.delivery_tag,
                                      multiple=True)
        except TypeError:
            break


class SendPing(object):

    def __init__(self, ID):

        # Create connection and channel.
        self.__connection, self.__channel = create_channel(PING_EXCHANGE)

    def publish(self, PID, counter, payload):

        # Publish 'ping' message.
        ping = create_ping(PID, counter, payload)
        self.__channel.publish(exchange=PING_EXCHANGE,
                               routing_key='ping',
                               body=msgpack.dumps(ping))

    def close(self):
        self.__channel.close()
        self.__connection.close()


class SendPong(object):

    @property
    def counter(self):
        return self.__counter

    def __init__(self, PID, ID, broadcasters, verbose):

        self.__counter = 0

        # Create event for terminating event loop.
        self.__queue = Queue.Queue()
        self.__run_event = threading.Event()
        self.__run_event.set()

        # Create thread for handling event loop.
        self.__event_loop = threading.Thread(target=self.__event_loop,
                                             args=(self.__run_event,
                                                   self.__queue, PID, verbose))
        self.__event_loop.daemon = True
        self.__event_loop.start()

    @staticmethod
    def __event_loop(run_event, queue, PID, verbose):

        # Create connections and channels.
        ping_connection, ping_channel = create_channel(PING_EXCHANGE)
        pong_connection, pong_channel = create_channel(PONG_EXCHANGE)

        # Create queue for receiving ping messages.
        queue_name = create_queue(ping_channel, PING_EXCHANGE, PING_TOPIC)

        # Wait for data
        counter = 0
        while run_event.is_set():
            try:
                for meth, props, payload in ping_channel.consume(queue_name,
                                                                 inactivity_timeout=0.1):
                    if payload and run_event.is_set():
                        # Publish 'pong' message.
                        ping = msgpack.loads(payload)
                        pong = create_pong(PID, ping)
                        pong_channel.publish(exchange=PONG_EXCHANGE,
                                             routing_key='pong',
                                             body=msgpack.dumps(pong))

                        ping_channel.basic_ack(delivery_tag=meth.delivery_tag,
                                               multiple=True)

                        counter += 1

                        if verbose:
                            s = 'PID %4i (rabbitmq): sent pong message %i'
                            s = s % (PID, pong['counter'])
                            print_if(verbose, s)

            except TypeError:
                pass

            except KeyboardInterrupt:
                break

        # Attempt to empty channel of data.
        ping_channel.queue_purge(queue=queue_name)
        empty_channel(ping_channel, queue_name)

        # Close connections.
        try:
            ping_channel.queue_purge(queue=queue_name)
            ping_channel.queue_delete(queue=queue_name)
            ping_channel.close()
            ping_connection.close()
            pong_channel.close()
            pong_connection.close()
        except:
            pass
        queue.put(counter)

    def close(self):

        self.__run_event.clear()
        self.__event_loop.join()
        self.__counter = self.__queue.get()


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

        self.__pings = multiprocessing.Queue()
        self.__pongs = multiprocessing.Queue()

        # Create event for terminating event loop.
        self.__run_event = multiprocessing.Event()
        self.__run_event.set()

        # Create thread for logging pings.
        self.__ping_loop = multiprocessing.Process(target=self.__event_loop,
                                                   args=(self.__run_event,
                                                         PING_EXCHANGE,
                                                         PING_TOPIC,
                                                         self.__pings))

        # Create thread for logging pongs.
        self.__pong_loop = multiprocessing.Process(target=self.__event_loop,
                                                   args=(self.__run_event,
                                                         PONG_EXCHANGE,
                                                         PONG_TOPIC,
                                                         self.__pongs))

        self.__ping_loop.daemon = True
        self.__pong_loop.daemon = True
        self.__ping_loop.start()
        self.__pong_loop.start()

    @staticmethod
    def __event_loop(run_event, exchange_name, topic, queue):

        # Create connections and channels.
        connection, channel = create_channel(exchange_name)

        # Create queue for receiving messages.
        queue_name = create_queue(channel, exchange_name, topic)
        time.sleep(0.25)
        channel.queue_purge(queue=queue_name)

        # Wait for messages.
        messages = list()
        while run_event.is_set():
            try:
                for meth, props, payload in channel.consume(queue_name,
                                                            inactivity_timeout=0.1):
                    if payload and run_event.is_set():
                        messages.append(payload)
                        channel.basic_ack(delivery_tag=meth.delivery_tag,
                                          multiple=True)

            except TypeError:
                pass

            except KeyboardInterrupt:
                break

        # Attempt to empty channel of data.
        channel.queue_purge(queue=queue_name)
        empty_channel(channel, queue_name)

        # Unpack messages.
        messages = [msgpack.loads(message) for message in messages]

        # Close connection.
        channel.queue_purge(queue=queue_name)
        channel.queue_delete(queue=queue_name)
        channel.close()
        connection.close()

        # Dump data into queue.
        queue.put(messages)

    def close(self):

        # Stop listening for data.
        self.__run_event.clear()
        time.sleep(0.1)
        self.__pings = self.__pings.get()
        self.__pongs = self.__pongs.get()
        self.__ping_loop.join()
        self.__pong_loop.join()

        # Ensure ping/pongs are stored in an identical format. Drop the payload
        # to save space.
        self.__pings, self.__pongs = format_ping_pongs(self.__pings,
                                                       self.__pongs)
