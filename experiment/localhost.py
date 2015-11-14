#!/usr/bin/python
"""

.. sectionauthor:: Asher Bender <a.bender.dev@gmail.com>
.. codeauthor:: Asher Bender <a.bender.dev@gmail.com>

"""

import os
import time
import pickle
import argparse
import textwrap
import multiprocessing
from bz2 import BZ2File

from common.common import ping
from common.common import pong
from common.common import make_payload
from common.common import log_ping_pong
from common.common import pings_to_dict
from common.common import pongs_to_dict


if __name__ == '__main__':

    # -------------------------------------------------------------------------
    #         Configure command-line options & parsing behaviour
    # -------------------------------------------------------------------------

    man = """

    """
    formatter_class = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(formatter_class=formatter_class,
                                     description=textwrap.dedent(man))

    msg = 'Directory to log data.'
    parser.add_argument('output', help=msg)

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
    print args.transport, args.packet, args.listeners, args.rate

    # Calculate delay based on packet size and data rate.
    msg_per_second = 1000000.0 * args.rate / float(args.packet)
    delay = 1.0 / msg_per_second

    # Create payload.
    payload = make_payload(args.packet)

    # Create event for controlling execution of processes.
    logger_queue = multiprocessing.Queue()
    ping_messages = multiprocessing.Queue()
    pong_messages = [multiprocessing.Queue() for i in range(args.listeners)]
    logger_run_event = multiprocessing.Event()
    pong_start_event = multiprocessing.Event()
    ping_start_event = multiprocessing.Event()
    ping_start_event.clear()
    pong_start_event.clear()
    logger_run_event.set()

    # -------------------------------------------------------------------------
    #                      Log ping and pong messages
    # -------------------------------------------------------------------------
    logger = multiprocessing.Process(target=log_ping_pong,
                                     args=(transport.LogPingPong,
                                           logger_run_event,
                                           logger_queue,
                                           args.transport,
                                           1, args.listeners))

    # Start the logging process.
    logger.daemon = False
    logger.start()
    time.sleep(0.1)

    # -------------------------------------------------------------------------
    #                        Broadcast PongMessage()
    # -------------------------------------------------------------------------
    pongers = list()
    for i in range(args.listeners):
        ponger = multiprocessing.Process(target=pong,
                                         args=(transport.SendPong, i, 1,
                                               pong_start_event,
                                               pong_messages[i],
                                               args.transport,
                                               args.verbose))

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
                                           ping_messages,
                                           payload,
                                           delay,
                                           args.transport,
                                           args.verbose))

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
    start_time = time.time()
    while True:
        try:
            time.sleep(0.1)

            if (args.time is not None):
                if time.time() - start_time > args.time:
                    break

        except KeyboardInterrupt:
            break

    # -------------------------------------------------------------------------
    #                           Shutdown services
    # -------------------------------------------------------------------------
    logger_run_event.clear()
    ping_start_event.clear()
    pong_start_event.clear()

    # Shutdown pinger.
    ping_messages = ping_messages.get()
    pinger.join()
    print 'Pings stopped.'

    # Shutdown pongers.
    for i, ponger in enumerate(pongers):
        pong_messages[i] = pong_messages[i].get()
        ponger.join()
    pong_messages = sum([lst for lst in pong_messages], [])
    print 'Pongs stopped.'

    # The payload is replicated across all messages. Duplicating the payload
    # does not aid analysis. To save significant space, the payload is removed
    # from the messages.
    pings = logger_queue.get()
    pongs = logger_queue.get()
    logger.join()
    print 'Stopped logging messages'

    # Create name of file.
    fname = '%s_%i_%i_%1.3f.pkl' % (args.transport,
                                    args.packet,
                                    args.listeners,
                                    args.rate)

    # Write logged data to file.
    data = dict()
    data['pings'] = pings_to_dict(pings)
    data['pongs'] = pongs_to_dict(pongs)
    with BZ2File(os.path.join(args.output, fname), 'w') as f:
        pickle.dump(data, f, protocol=2)

    # Write process data to file.
    data = dict()
    data['pings'] = pings_to_dict(ping_messages)
    data['pongs'] = pongs_to_dict(pong_messages)
    with BZ2File(os.path.join(args.output, 'process', fname), 'w') as f:
        pickle.dump(data, f, protocol=2)

    print 'Pings sent %i. Pings logged %i.' % (len(ping_messages), len(pings))
    print 'Pongs sent %i. Pongs logged %i.' % (len(pong_messages), len(pongs))
