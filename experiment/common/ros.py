from __future__ import absolute_import

import time
import rospy
import msgpack
import multiprocessing
from std_msgs.msg import String

from .common import print_if
from .common import get_utc_string
from .common import format_ping_pongs


class SendPing(object):

    def __init__(self, ID):

        # Create publisher.
        rospy.init_node('pinger_%i' % ID, anonymous=True)
        self.__publisher = rospy.Publisher('ping', String, queue_size=100)

    def publish(self, PID, counter, payload):

        # Create message
        message = {'ping_PID': PID,
                   'counter': counter,
                   'payload': payload,
                   'ping_time': get_utc_string()}

        # Publish message.
        try:
            self.__publisher.publish(msgpack.dumps(message))
        except rospy.ROSException:
            pass

    def close(self):
        pass


class SendPong(object):

    def __init__(self, PID, ID, broadcasters, verbose, max_chars):

        # Create message listener and broadcaster for Pings and Pongs.
        rospy.init_node('ponger_%i' % ID, anonymous=True)
        self.__broadcaster = rospy.Publisher('pong', String, queue_size=100)

        def callback(data):
            """Repack Ping data as a Pong."""

            ping = msgpack.loads(data.data)
            pong = {'ping_PID': ping['ping_PID'],
                    'counter': ping['counter'],
                    'pong_PID': PID,
                    'payload': ping['payload'],
                    'pong_time': get_utc_string()}

            try:
                self.__broadcaster.publish(msgpack.dumps(pong))
            except rospy.ROSException:
                pass

            if verbose:
                s = 'PID %4i (ros): sent pong message %i'
                s = s % (PID, pong['counter'])
                print_if(verbose, s, max_chars)

        self.__listener = rospy.Subscriber('ping', String, callback)

    def close(self):

        self.__listener.unregister()
        rospy.signal_shutdown('')


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

        # Create queues for storing messages.
        self.__pings = multiprocessing.Queue()
        self.__pongs = multiprocessing.Queue()

        # Create event for controlling execution of processes.
        self.__start_event = multiprocessing.Event()
        self.__start_event.set()

        self.__proc = multiprocessing.Process(target=self.__event_loop,
                                              args=(self.__start_event,
                                                    self.__pings,
                                                    self.__pongs))
        self.__proc.daemon = True
        self.__proc.start()

    @staticmethod
    def __event_loop(run_event, ping_queue, pong_queue):

        # Store pings and pongs in local lists.
        ping_list = list()
        pong_list = list()
        rospy.init_node('logger', anonymous=True)
        def ping_callback(data): ping_list.append(msgpack.loads(data.data))
        def pong_callback(data): pong_list.append(msgpack.loads(data.data))
        ping_listener = rospy.Subscriber('ping', String, ping_callback)
        pong_listener = rospy.Subscriber('pong', String, pong_callback)

        # Wait for stop signal.
        while run_event.is_set():
            time.sleep(0.1)

        # Shut down ROS.
        ping_listener.unregister()
        pong_listener.unregister()
        rospy.signal_shutdown('')

        # Pipe ping and pong lists out of process using queues.
        ping_queue.put(ping_list)
        pong_queue.put(pong_list)

    def close(self):

        # End process and retrieve message data.
        self.__start_event.clear()
        self.__pings = self.__pings.get()
        self.__pongs = self.__pongs.get()
        self.__proc.join()

        # Ensure ping/pongs are stored in an identical format. Drop the payload
        # to save space.
        self.__pings, self.__pongs = format_ping_pongs(self.__pings,
                                                       self.__pongs)
