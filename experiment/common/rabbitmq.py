from __future__ import absolute_import

import time
import pika
import Queue
import msgpack
import threading

from .common import print_if
from .common import get_utc_string

PORT = 5672
HOSTNAME = '10.0.0.101'
HOSTNAME = 'localhost'
PING_EXCHANGE = 'ping'
PONG_EXCHANGE = 'pong'


class SendPing(object):

    def __init__(self):

        # Create connection.
        credentials = pika.PlainCredentials('test', 'test')
        parameters = pika.ConnectionParameters(host=HOSTNAME, port=PORT,
                                               credentials=credentials)
        self.__connection = pika.BlockingConnection(parameters)

        # Establish channel.
        self.__channel = self.__connection.channel()
        self.__channel.exchange_declare(exchange=PING_EXCHANGE, type='topic')

    def publish(self, PID, counter, payload):

        # Create message
        message = {'ping_PID': PID,
                   'counter': counter,
                   'payload': payload,
                   'ping_time': get_utc_string()}

        # Publish data to exchange.
        #
        # Note: The 'mandatory' flag tells the server how to react if the
        #       message cannot be routed to a queue. If this flag is set, the
        #       server will return an unroutable message with a Return
        #       method. If this flag is zero, the server silently drops the
        #       message
        #
        #       The 'immediate' tells the server how to react if the message
        #       cannot be routed to a queue consumer immediately. If this flag
        #       is set, the server will return an undeliverable message with a
        #       Return method. If this flag is zero, the server will queue the
        #       message, but with no guarantee that it will ever be consumed.
        #
        #       Both 'mandatory' and 'immediate' default to False. They are
        #       explicitly defined here to document publishing behaviour.
        #
        self.__channel.basic_publish(exchange=PING_EXCHANGE,
                                     routing_key='',
                                     mandatory=False,
                                     immediate=False,
                                     body=msgpack.dumps(message))

    def close(self):
        self.__connection.close()


class SendPong(object):

    def __init__(self, PID, verbose, max_chars):

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

        # Create connection.

        # Create connection.
        credentials = pika.PlainCredentials('test', 'test')
        parameters = pika.ConnectionParameters(host=HOSTNAME, port=PORT,
                                               credentials=credentials)

        # Establish ping channel.
        ping_connection = pika.BlockingConnection(parameters)
        ping_channel = ping_connection.channel()
        ping_channel.exchange_declare(exchange=PING_EXCHANGE, type='topic')
        result = ping_channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        ping_channel.queue_bind(exchange=PING_EXCHANGE, queue=queue_name,
                                routing_key='#')

        # Establish pong channel.
        pong_connection = pika.BlockingConnection(parameters)
        pong_channel = pong_connection.channel()
        pong_channel.exchange_declare(exchange=PONG_EXCHANGE, type='topic')

        try:
            while run_event.is_set():
                method, header, payload = ping_channel.basic_get(queue=queue_name,
                                                                 no_ack=True)
                if method:
                    ping = msgpack.loads(payload)
                    pong = {'ping_PID': ping['ping_PID'],
                            'counter': ping['counter'],
                            'pong_PID': PID,
                            'payload': ping['payload'],
                            'pong_time': get_utc_string()}

                    pong_channel.basic_publish(exchange=PONG_EXCHANGE,
                                               routing_key='',
                                               mandatory=False,
                                               immediate=False,
                                               body=msgpack.dumps(pong))

                    if verbose:
                        s = 'PID %4i (RabbitMQ): sent pong message %i'
                        s = s % (PID, pong['counter'])
                        print_if(verbose, s, max_chars)

        except KeyboardInterrupt:
            pass

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

    def __init__(self):

        self.__pings = Queue.Queue()
        self.__pongs = Queue.Queue()

        # Create event for terminating event loop.
        self.__run_event = threading.Event()
        self.__run_event.set()

        # Create thread for logging pings.
        self.__ping_loop = threading.Thread(target=self.__event_loop,
                                            args=(self.__run_event,
                                                  PING_EXCHANGE,
                                                  self.__pings))

        # Create thread for logging pongs.
        self.__pong_loop = threading.Thread(target=self.__event_loop,
                                            args=(self.__run_event,
                                                  PONG_EXCHANGE,
                                                  self.__pongs))

        self.__ping_loop.daemon = True
        self.__pong_loop.daemon = True
        self.__ping_loop.start()
        self.__pong_loop.start()

    @staticmethod
    def __event_loop(run_event, exchange, queue):

        # Create connection.
        parameters = pika.ConnectionParameters(host=HOSTNAME)

        # Establish ping channel.
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.exchange_declare(exchange=exchange, type='topic')
        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=exchange, queue=queue_name,
                           routing_key='#')

        try:
            while run_event.is_set():
                method, header, payload = channel.basic_get(queue=queue_name,
                                                            no_ack=True)
                if method:
                    queue.put(msgpack.loads(payload))

        except KeyboardInterrupt:
            pass

        channel.close()
        connection.close()

    def close(self):

        # Stop listening for data.
        self.__run_event.clear()
        self.__ping_loop.join()
        self.__pong_loop.join()
        self.__pings.put('END')
        self.__pongs.put('END')
        time.sleep(0.1)

        # Convert queue to a lists.
        self.__pings = [item for item in iter(self.__pings.get, 'END')]
        self.__pongs = [item for item in iter(self.__pongs.get, 'END')]
