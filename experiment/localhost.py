#!/usr/bin/python
"""

.. sectionauthor:: Asher Bender <a.bender.dev@gmail.com>
.. codeauthor:: Asher Bender <a.bender.dev@gmail.com>

"""

import time
import pickle
import argparse
import datetime
import textwrap
import multiprocessing
from bz2 import BZ2File
from collections import OrderedDict

from common.common import ping
from common.common import pong
from common.common import make_payload


if __name__ == '__main__':

    # -------------------------------------------------------------------------
    #         Configure command-line options & parsing behaviour
    # -------------------------------------------------------------------------

    man = """

    """
    formatter_class = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(formatter_class=formatter_class,
                                     description=textwrap.dedent(man))

    msg = 'Path to log data.'
    parser.add_argument('fname', help=msg)

    msg = 'Number of listeners to launch.'
    parser.add_argument('--listeners', type=int, help=msg, default=1)

    msg = 'Packet size in bytes.'
    parser.add_argument('--packet', type=int, help=msg, default=1000)

    msg = 'Data rate in MB/s.'
    parser.add_argument('--rate', type=float, help=msg, default=5)

    choices = ['lcm', 'mcl', 'pyre', 'rabbitmq', 'ros', 'zmq']
    msg = "Underlying transport to use during test. Choose from '%r'."
    parser.add_argument('--transport', choices=choices, default='mcl')

    msg = 'Time to send pings.'
    parser.add_argument('--time', type=float, help=msg, default=None)

    msg = 'Print  messages to screen.'
    parser.add_argument('--verbose', action='store_true', default=False,
                        help=msg)

    msg = 'Maximum number of character to print to the screen.'
    parser.add_argument('--length', type=int, help=msg, default=None)

    # Get arguments from the command-line.
    args = parser.parse_args()

    # Load transport method.
    if args.transport == 'lcm':
        import common.lcm as transport
    elif args.transport == 'mcl':
        import common.mcl as transport
    elif args.transport == 'pyre':
        import common.pyre as transport
    elif args.transport == 'rabbitmq':
        import common.rabbitmq as transport
    elif args.transport == 'ros':
        import common.ros as transport
    elif args.transport == 'zmq':
        import common.zmq as transport

    # -------------------------------------------------------------------------
    #                             Start logging
    # -------------------------------------------------------------------------

    # Calculate delay based on packet size and data rate.
    msg_per_second = 1000000.0 * args.rate / float(args.packet)
    delay = 1.0 / msg_per_second

    # Create payload.
    payload = make_payload(args.packet)

    # Create event for controlling execution of processes.
    ping_start_event = multiprocessing.Event()
    pong_start_event = multiprocessing.Event()
    ping_start_event.clear()
    pong_start_event.clear()

    # Start logging pings and pongs.
    print 'Logging messages...'
    logger = transport.LogPingPong(1, args.listeners)
    time.sleep(0.1)

    # -------------------------------------------------------------------------
    #                        Broadcast PongMessage()
    # -------------------------------------------------------------------------

    # Create process for broadcasting PongMessages()
    pongers = list()
    for i in range(args.listeners):
        ponger = multiprocessing.Process(target=pong,
                                         args=(transport.SendPong, i, 1,
                                               pong_start_event,
                                               args.transport,
                                               args.verbose,
                                               args.length))

        # Start the broadcasting process.
        pongers.append(ponger)
        ponger.daemon = False
        ponger.start()

    # Synchronise pong services.
    print 'Starting pongs...'
    pong_start_event.set()
    time.sleep(0.1)

    # -------------------------------------------------------------------------
    #                        Broadcast PingMessage()
    # -------------------------------------------------------------------------

    # Create process for broadcasting PingMessages()
    pinger = multiprocessing.Process(target=ping,
                                     args=(transport.SendPing, 0,
                                           ping_start_event,
                                           payload,
                                           delay,
                                           args.transport,
                                           args.verbose,
                                           args.length))

    # Start the broadcasting process.
    print 'Starting pings...'
    pinger.daemon = True
    pinger.start()
    time.sleep(0.1)
    ping_start_event.set()

    # -------------------------------------------------------------------------
    #                      Wait for experiment to end
    # -------------------------------------------------------------------------

    # Wait for the user to terminate the process or timeout.
    start_time = datetime.datetime.utcnow()
    while True:
        try:
            time.sleep(0.1)

            if (args.time is not None):
                elapsed_time = datetime.datetime.utcnow() - start_time
                if elapsed_time.total_seconds() > args.time:
                    break

        except KeyboardInterrupt:
            break

    # -------------------------------------------------------------------------
    #                           Shutdown services
    # -------------------------------------------------------------------------

    # Shutdown pinger.
    ping_start_event.clear()
    pinger.join()
    print 'Pings stopped.'

    # Shutdown pongers.
    pong_start_event.clear()
    for ponger in pongers:
        ponger.join()
    print 'Pongs stopped.'

    # Stop logging pings and pongs.
    logger.close()
    print 'Stopped logging messages'

    # The payload is replicated across all messages. Duplicating the payload
    # does not aid analysis. To save significant space, the payload is removed
    # from the messages.
    data = OrderedDict()
    data['pings'] = logger.pings
    data['pongs'] = logger.pongs

    # Write data to file.
    with BZ2File(args.fname, 'w') as f:
        pickle.dump(data, f, protocol=2)
