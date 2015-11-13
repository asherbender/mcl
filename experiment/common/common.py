import os
import time
import socket
import numpy as np
from collections import OrderedDict

ISO_FMT = '%Y-%m-%dT%H:%M:%S.%f'
LOCALHOST = True


def get_hostname():
    """Get the hostname of the current device.

    Returns:
      str: hostname of device.

    """
    try:
        hostname = socket.gethostname()
    except:
        hostname = 'unknown'

    return hostname


def print_if(verbose, string, max_chars):
    """Conditionally print string to std.out

    Args:
      verbose (bool): if set to `True` the string will be printed. Otherwise do
          not print.
      string (str): string to pring.
      max_chars (int): maximum number of characters to print.

    """
    if verbose:
        if max_chars is not None:
            print string[:max(0, max_chars - 3)] + '...'
        else:
            print string


def set_process_name(name):
    """Function for setting the name of new processes.

    Rename the process to:

        <old name> -> <name>

    Args:
      text (str): Name to set process.

    """

    # Set the name of a new process if 'setproctitle' exists.
    try:
        from setproctitle import setproctitle as setproctitle
        name = '/usr/bin/python ' + name
        setproctitle(name)

    # If 'setproctitle' does not exist. Do nothing.
    except:
        pass


def make_payload(size):
    """Generate a test payload.

    Generate a string containing an increasing sequence of integers. An example
    of this output is:

        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 1

    The size of the payload is in bytes and the length of the string (note that
    the final integer is truncated). It is NOT the number of integers recorded
    in the string.

    Args:
      size (int): Size of test payload in bytes.

    Returns:
      str: The test payload is returned as a string of length ``size`` bytes,
           containing a sequence of increasing integers.

    """

    payload = ''
    counter = 1
    while True:
        payload += '%i, ' % counter
        counter += 1
        if len(payload) >= size:
            break

    return payload[:size]


def create_ping(PID, counter, payload):
    """Generate a ping dictionary.

    Generate a ping dictionary of the form:

        {'ping_PID': <int>,
         'counter': <int>,
         'payload': <str>,
         'ping_time': <str>}

    Args:
      PID (int): PID of ping process.
      counter (int): message counter.
      payload (str): payload of ping message.

    Returns:
      dict: Ping dictionary

    """
    return {'ping_PID': PID,
            'counter': counter,
            'payload': payload,
            'ping_time': time.time()}


def create_pong(PID, ping):
    """Generate a pong dictionary from a ping dictionary.

    Generate a pong dictionary (from a ping dictionary) of the form:

        {'ping_PID': <int>,
         'counter': <int>,
         'pong_PID': <int>,
         'payload': <str>,
         'pong_time': <str>}

    Args:
      PID (int): PID of pong process.
      ping (dict): ping dictionary.

    Returns:
      dict: Pong dictionary

    """
    return {'ping_PID': ping['ping_PID'],
            'counter': ping['counter'],
            'pong_PID': PID,
            'payload': ping['payload'],
            'pong_time': time.time()}


def ping(SendPing, ID, start_event, payload, delay, transport, verbose, max_chr):
    """Standardised function for broadcasting ping messages.

    Args:
      SendPing (object): Object for sending ping messages.
      ID (int): ID of this ping thread/process.
      start_event (multiprocessing.Event): Flag for terminating pings.
      payload (str): payload to send in ping messages.
      delay (float): delay between ping messages.
      transport (str): name of transport.
      verbose (bool): if set to `True` a message will be printed for each ping
          transmitted.
      max_chr (int): maximum number of characters to print to the screen.

    """

    # Attempt to set process name.
    PID = os.getpid()
    proc_name = 'pinger (%s, %i): ID %i' % (transport, PID, ID)
    set_process_name(proc_name)

    # Create message broadcaster for ping messages.
    ping = SendPing(ID)

    # Wait until start event has been triggered.
    print_if(verbose, 'PID %4i: starting pings' % PID, max_chr)
    while not start_event.is_set():
        pass

    # Update data and send at a constant rate (until user cancels).
    try:
        counter = 0
        while start_event.is_set():
            time_before = time.time()

            ping.publish(PID, counter, payload)

            if verbose:
                msg = 'PID %4i (%s): sent ping message %i'
                msg = msg % (PID, transport, counter)
                print_if(verbose, msg, max_chr)

            counter += 1

            # Sleep for requested length of time. Attempt to correct for the
            # length of time required to construct and send the message.
            while (time.time() - time_before) < delay:
                pass

    # Terminate thread on keyboard cancel.
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print str(e)

    print counter

    ping.close()
    print_if(verbose, 'PID %4i (%s): exiting' % (PID, transport), max_chr)


def pong(SendPong, ID, broadcasters, start_event, transport, verbose, max_chr):
    """Standardised function for broadcasting pong messages.

    Args:
      SendPong (object): Object for sending pong messages.
      ID (int): ID of this pong thread/process.
      broadcasters (int): total number of broadcasters in the system.
      start_event (multiprocessing.Event): Flag for terminating pongs.
      transport (str): name of transport.
      verbose (bool): if set to `True` a message will be printed for each pong
          transmitted.
      max_chr (int): maximum number of characters to print to the screen.

    """

    # Attempt to set process name.
    PID = os.getpid()
    proc_name = 'ponger (%s, %i): ID %i' % (transport, PID, ID)
    set_process_name(proc_name)

    # Create message broadcaster for pong messages.
    ponger = SendPong(PID, ID, broadcasters, verbose, max_chr)

    # Wait until start event has been triggered.
    msg = 'PID %4i (%s): starting pongs' % (PID, transport)
    print_if(verbose, msg, max_chr)
    while not start_event.is_set():
        pass

    # Wait for kill signal.
    try:
        while start_event.is_set():
            time.sleep(0.1)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print str(e)

    ponger.close()
    print_if(verbose, 'PID %4i (%s): exiting' % (PID, transport), max_chr)


def log_ping_pong(LogPingPong, run_event, queue, transport, pingers, pongers):

    # Attempt to set process name.
    PID = os.getpid()
    proc_name = 'logger (%s, %i): %i pingers, %i pongers'
    proc_name = proc_name % (transport, PID, pingers, pongers)
    set_process_name(proc_name)

    # Create logger.
    logger = LogPingPong(pingers, pongers)

    # Wait for kill signal.
    try:
        while run_event.is_set():
            time.sleep(0.1)

    # Terminate thread on keyboard cancel.
    except KeyboardInterrupt:
        pass

    # Stop logging pings and pongs.
    logger.close()
    queue.put(logger.pings)
    queue.put(logger.pongs)


def format_ping_pongs(pings, pongs):
    """Format a list of pings and a list of pongs into a standard format.

    Args:
      pings (list): ping dictionaries.
      pongs (list): pong dictionaries.

    Returns:
      tuple: (pings, pongs) each list contains pings/pongs ordered by counter
          (time). The payload is removed from each message.

    """

    # Ensure pings are stored in an identical format - drop the payload to save
    # space.
    formatted_pings = list()
    for ping in pings:
        formatted_pings.append({'ping_PID': int(ping['ping_PID']),
                                'counter': int(ping['counter']),
                                'ping_time': float(ping['ping_time'])})

    # Ensure pongs are stored in an identical format - drop the payload to save
    # space.
    formatted_pongs = list()
    for pong in pongs:
        formatted_pongs.append({'ping_PID': int(pong['ping_PID']),
                                'counter': int(pong['counter']),
                                'pong_PID': int(pong['pong_PID']),
                                'pong_time': float(pong['pong_time'])})

    # Sort ping/pongs in order of counter (time).
    pings = sorted(formatted_pings, key=lambda ping: ping['counter'])
    pongs = sorted(formatted_pongs, key=lambda pong: pong['counter'])

    return pings, pongs


def pings_to_dict(pings):
    """Convert list of ping data into an ordered dictionary

    Convert list of ping dictionaries:

       [{'counter': <int>, 'ping_time': <datetime.datetime>, 'ping_PID': <int>},
        ...,
        {'counter': <int>, 'ping_time': <datetime.datetime>, 'ping_PID': <int>}]

    into an ordered dictionary.

       ping_data[ping_PID][counter] = <datetime.datetime>

    Args:
      pings (list): ping dictionaries.

    Returns:
      OrderedDict: ping data

    """

    ping_data = dict()
    for ping in pings:
        if ping['ping_PID'] not in ping_data:
            ping_data[ping['ping_PID']] = dict()

        ping_data[ping['ping_PID']][ping['counter']] = ping['ping_time']

    # Order pings by counter.
    for key, value in ping_data.iteritems():
        value = sorted(value.iteritems(), key=lambda item: item[0])
        ping_data[key] = OrderedDict(value)

    # Order ping services by PID.
    ping_data = sorted(ping_data.iteritems(), key=lambda item: item[0])
    return OrderedDict(ping_data)


def pongs_to_dict(pongs):
    """Convert list of pong data into an ordered dictionary

    Convert list of pong dictionaries:

       [{'pong_PID': <int>, 'counter': <int>, 'ping_PID': <int>, 'pong_time': <datetime.datetime>},
        ...,
        {'pong_PID': <int>, 'counter': <int>, 'ping_PID': <int>, 'pong_time': <datetime.datetime>}]

    into an ordered dictionary.

       pong_data[ping_PID][pong_PID][counter] = <datetime.datetime>

    Args:
      pings (list): pong dictionaries.

    Returns:
      OrderedDict: pong data

    """

    pong_data = dict()
    for pong in pongs:
        ping_PID = pong['ping_PID']
        pong_PID = pong['pong_PID']

        if ping_PID not in pong_data:
            pong_data[ping_PID] = dict()

        if pong_PID not in pong_data[ping_PID]:
            pong_data[ping_PID][pong_PID] = dict()

        pong_data[ping_PID][pong_PID][pong['counter']] = pong['pong_time']

    # Order pongs by counter.
    for ping_PID in pong_data.iterkeys():
        for pong_PID in pong_data[ping_PID].iterkeys():
            data = pong_data[ping_PID][pong_PID].iteritems()
            data = sorted(data, key=lambda item: item[0])
            pong_data[ping_PID][pong_PID] = OrderedDict(data)

    # Order pongs services by PID.
    for pong_PID, value in pong_data.iteritems():
        data = sorted(value.iteritems(), key=lambda item: item[0])
        pong_data[pong_PID] = OrderedDict(data)

    # Order ping services by PID.
    data = sorted(pong_data.iteritems(), key=lambda item: item[0])
    return OrderedDict(data)


def message_stats(payload, data):
    """Calculate statistics in ping/pong data.

    Calculate statistics for ping/pong data in the form:

        stats[PID] = {'number_messages': <int>,
                      'data_transfered': <int>,
                      'duration': <float>,
                      'data_rate': <float>}

    where:
        - `number_messages` are the number of ping/pong messages that were
          transferred.
        - `data_transfered` is the total number of ping/pong payload bytes
          transferred.
        - `duration` time between first and last ping/pong message in seconds.
        - `data_rate` bandwidth of ping/pong messages in MB/s

    Args:
      payload (int): size of payload.
      data (OrderedDict): ping/pong data.

    Returns:
      dict: summary of ping data

    """

    stats = dict()
    for ping_PID, counter_timestamp in data.iteritems():

        # Calculate amount of data transfered in Mb.
        number_messages = len(counter_timestamp)
        data_transfered = float(payload) * number_messages / 1000000.0

        # Calculate duration of transitions.
        duration = data[ping_PID].values()[-1] - data[ping_PID].values()[0]

        # Data transfered in Mb/s
        data_rate = data_transfered / duration

        stats[ping_PID] = {'number_messages': number_messages,
                           'data_transfered': data_transfered,
                           'duration': duration,
                           'data_rate': data_rate}

    return stats


def network_stats(ping_data, pong_data):
    """Calculate statistics in pong data.

    Calculate statistics for ping data in the form:

        stats[ping_PID][pong_PID] = {'number_messages': <int>,
                                     'data_transfered': <int>,
                                     'duration': <float>,
                                     'data_rate': <float>,
                                     'latency': <float>,
                                     'dropped': <int>}

    where:
        - `number_messages` are the number of pong messages that were
          transferred.
        - `data_transfered` is the total number of pong payload bytes
          transferred.
        - `duration` time between first and last pong message in seconds.
        - `data_rate` bandwidth of pong messages in MB/s

    Args:
      payload (int): size of payload.
      ping_data (OrderedDict): ping data.
      pong_data (OrderedDict): pong data.

    Returns:
      dict: summary of pong data

    """

    # Ensure all ping processes are present in the pong data.
    mg = 'Ping PIDs not equal: %r != %r' % (ping_data.keys(), pong_data.keys())
    assert ping_data.keys() == pong_data.keys(), mg

    total_latency = list()
    total_dropped = list()

    # Iterate through ping processes.
    for ping_PID in ping_data.iterkeys():
        pingcounters = set(ping_data[ping_PID].keys())

        # Iterate through pong processes.
        for pong_PID in pong_data[ping_PID].iterkeys():

            # Create set of counters from ping and pongs.
            counters = pingcounters | set(pong_data[ping_PID][pong_PID].keys())

            dropped = 0
            latency = list()
            for counter in counters:
                if ((counter in ping_data[ping_PID]) and
                    (counter in pong_data[ping_PID][pong_PID])):
                    ping_time = ping_data[ping_PID][counter]
                    pong_time = pong_data[ping_PID][pong_PID][counter]
                    latency.append(pong_time - ping_time)
                else:
                    dropped += 1

            latency = np.array(latency)
            latency = latency.mean()
            total_latency.append(latency)
            total_dropped.append(dropped)

    return {'latency': float(np.array(total_latency).mean()),
            'dropped': float(np.array(total_dropped).mean())}
