from __future__ import absolute_import

import pika
import time
import msgpack
import threading
import multiprocessing

from .common import print_if
from .common import get_utc_string
from .common import utc_str_to_datetime

PORT = 5672
LOCALHOST = True
if LOCALHOST:
    HOSTNAME = 'localhost'
else:
    HOSTNAME = '10.0.0.101'
PING_EXCHANGE = 'ping'
PONG_EXCHANGE = 'pong'


def create_channel(exchange_name):

        # Create connection parameters.
        credentials = pika.PlainCredentials('test', 'test')
        parameters = pika.ConnectionParameters(host=HOSTNAME,
                                               port=PORT,
                                               credentials=credentials)

        # Create connection.
        connection = pika.BlockingConnection(parameters)

        # Establish channel.
        channel = connection.channel()
        channel.exchange_declare(exchange=exchange_name,
                                 exchange_type='fanout')

        return connection, channel


def create_queue(channel, exchange_name):

    # Create queue for ping messages.
    result = channel.queue_declare(exclusive=True, auto_delete=True)
    queue_name = result.method.queue

    # Bind queue to exchange.
    channel.queue_bind(exchange=exchange_name,
                       queue=queue_name)

    return queue_name


def channel_get(channel, queue_name):

    method, header, payload = channel.basic_get(queue=queue_name)

    if method:
        channel.basic_ack(delivery_tag=method.delivery_tag, multiple=True)
        return payload
    else:
        return None


class SendPing(object):

    def __init__(self, ID):

        # Create connection and channel.
        self.__connection, self.__channel = create_channel(PING_EXCHANGE)

    def publish(self, PID, counter, payload):

        # Create message
        message = {'ping_PID': PID,
                   'counter': counter,
                   'payload': payload,
                   'ping_time': get_utc_string()}

        # Publish data to exchange.
        try:
            self.__channel.publish(exchange=PING_EXCHANGE,
                                   routing_key='',
                                   body=msgpack.dumps(message))
        except:
            pass

    def close(self):
        self.__channel.close()
        self.__connection.close()


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

        # Create connections and channels.
        ping_connection, ping_channel = create_channel(PING_EXCHANGE)
        pong_connection, pong_channel = create_channel(PONG_EXCHANGE)

        # Create queue for receiving ping messages.
        queue_name = create_queue(ping_channel, PING_EXCHANGE)

        try:
            while run_event.is_set():
                payload = channel_get(ping_channel, queue_name)

                if payload:
                    ping = msgpack.loads(payload)
                    pong = {'ping_PID': ping['ping_PID'],
                            'counter': ping['counter'],
                            'pong_PID': PID,
                            'payload': ping['payload'],
                            'pong_time': get_utc_string()}

                    pong_channel.publish(exchange=PONG_EXCHANGE,
                                         routing_key='',
                                         body=msgpack.dumps(pong))

                    if verbose:
                        s = 'PID %4i (rabbitmq): sent pong message %i'
                        s = s % (PID, pong['counter'])
                        print_if(verbose, s, max_chars)

        except KeyboardInterrupt:
            pass

        # Close connections.
        ping_channel.queue_purge(queue=queue_name)
        ping_channel.queue_delete(queue=queue_name)
        ping_channel.close()
        ping_connection.close()
        pong_channel.close()
        pong_connection.close()

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

        self.__pings = multiprocessing.Queue()
        self.__pongs = multiprocessing.Queue()

        # Create event for terminating event loop.
        self.__run_event = multiprocessing.Event()
        self.__run_event.set()

        # Create thread for logging pings.
        self.__ping_loop = multiprocessing.Process(target=self.__event_loop,
                                                   args=(self.__run_event,
                                                         PING_EXCHANGE,
                                                         self.__pings))

        # Create thread for logging pongs.
        self.__pong_loop = multiprocessing.Process(target=self.__event_loop,
                                                   args=(self.__run_event,
                                                         PONG_EXCHANGE,
                                                         self.__pongs))

        self.__ping_loop.daemon = True
        self.__pong_loop.daemon = True
        self.__ping_loop.start()
        self.__pong_loop.start()

    @staticmethod
    def __event_loop(run_event, exchange_name, queue):

        # Create connections and channels.
        connection, channel = create_channel(exchange_name)

        # Create queue for receiving messages.
        queue_name = create_queue(channel, exchange_name)
        time.sleep(0.25)
        channel.queue_purge(queue=queue_name)

        # Wait for messages.
        messages = list()
        try:
            while run_event.is_set():
                payload = channel_get(channel, queue_name)
                if payload:
                    messages.append(payload)

        except KeyboardInterrupt:
            pass

        # Purge messages in queue.
        while True:
            query = channel.queue_declare(queue=queue_name, passive=True)
            if query.method.message_count > 0:
                payload = channel_get(channel, queue_name)
            else:
                break

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
