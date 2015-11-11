import os
import time
import socket
import datetime

ISO_FMT = '%Y-%m-%dT%H:%M:%S.%f'
LOCALHOST = False


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
        from setproctitle import getproctitle as getproctitle
        from setproctitle import setproctitle as setproctitle

        current_name = getproctitle()
        name = current_name + ' -> ' + name
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


def get_utc_string():
    """Get current UTC time as an ISO formatted string.

    Returns:
      str: ISO formatted time.

    """

    # Could implement:
    #
    #     datetime.datetime.utcnow().isoformat()
    #
    # Note that isoformat return a string representing the date and time in ISO
    # 8601 format, YYYY-MM-DDTHH:MM:SS.mmmmmm or, if microsecond is 0,
    # YYYY-MM-DDTHH:MM:SS.
    #
    # To avoid this conditional behaviour ALWAY include microseconds:
    #
    return datetime.datetime.utcnow().strftime(ISO_FMT)


def utc_str_to_datetime(s):
    """Convert ISO-time string into a datetime.datetime object.

    Args:
      s (str): ISO formatted time.

    Returns:
      datetime.datetime: time object.

    """
    return datetime.datetime.strptime(s, ISO_FMT)


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
            'ping_time': get_utc_string()}


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
            'pong_time': get_utc_string()}


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
    proc_name = 'pinger %i (%s)' % (PID, transport)
    set_process_name(proc_name)

    # Create message broadcaster for PingMessage().
    ping = SendPing(ID)

    # Wait until start event has been triggered.
    print_if(verbose, 'PID %4i: starting pings' % PID, max_chr)
    while not start_event.is_set():
        pass

    # Update data and send at a constant rate (until user cancels).
    try:
        counter = 0
        while start_event.is_set():
            time_before = datetime.datetime.utcnow()

            ping.publish(PID, counter, payload)

            if verbose:
                msg = 'PID %4i (%s): sent ping message %i'
                msg = msg % (PID, transport, counter)
                print_if(verbose, msg, max_chr)

            counter += 1

            # Sleep for requested length of time. Attempt to correct for the
            # length of time required to construct and send the message.
            time_after = datetime.datetime.utcnow()
            elapsed_time = (time_after - time_before).total_seconds()
            sleep_time = max(0, delay - elapsed_time)
            time.sleep(sleep_time)

    # Terminate thread on keyboard cancel.
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print str(e)

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
    proc_name = 'pinger %i (%s)' % (PID, transport)
    set_process_name(proc_name)

    # Wait until start event has been triggered.
    msg = 'PID %4i (%s): starting pongs' % (PID, transport)
    print_if(verbose, msg, max_chr)
    ponger = SendPong(PID, ID, broadcasters, verbose, max_chr)
    while not start_event.is_set():
        pass

    try:
        while start_event.is_set():
            time.sleep(0.1)

    # Terminate thread on keyboard cancel.
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print str(e)

    ponger.close()
    print_if(verbose, 'PID %4i (%s): exiting' % (PID, transport), max_chr)


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
                                'ping_time': utc_str_to_datetime(ping['ping_time'])})

    # Ensure pongs are stored in an identical format - drop the payload to save
    # space.
    formatted_pongs = list()
    for pong in pongs:
        formatted_pongs.append({'ping_PID': int(pong['ping_PID']),
                                'counter': int(pong['counter']),
                                'pong_PID': int(pong['pong_PID']),
                                'pong_time': utc_str_to_datetime(pong['pong_time'])})

    # Sort ping/pongs in order of counter (time).
    pings = sorted(formatted_pings, key=lambda ping: ping['counter'])
    pongs = sorted(formatted_pongs, key=lambda pong: pong['counter'])

    return ping, pong
