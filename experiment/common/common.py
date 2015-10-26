import os
import time
import socket
import datetime


def get_hostname():

    try:
        hostname = socket.gethostname()
    except:
        hostname = 'unknown'

    return hostname


def print_if(verbose, string, max_chars):

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
    return datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')


def ping(SendPing, start_event, payload, delay, transport, verbose, max_chars):

    # Attempt to set process name.
    PID = os.getpid()
    proc_name = 'pinger %i (%s)' % (PID, transport)
    set_process_name(proc_name)

    # Create message broadcaster for PingMessage().
    ping = SendPing()

    # Wait until start event has been triggered.
    print_if(verbose, 'PID %4i: starting pings' % PID, max_chars)
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
                print_if(verbose, msg, max_chars)

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
    print_if(verbose, 'PID %4i (%s): exiting' % (PID, transport), max_chars)


def pong(SendPong, start_event, transport, verbose, max_chars):

    # Attempt to set process name.
    PID = os.getpid()
    proc_name = 'pinger %i (%s)' % (PID, transport)
    set_process_name(proc_name)

    # Wait until start event has been triggered.
    msg = 'PID %4i (%s): starting pongs' % (PID, transport)
    print_if(verbose, msg, max_chars)
    ponger = SendPong(PID, verbose, max_chars)
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
    print_if(verbose, 'PID %4i (%s): exiting' % (PID, transport), max_chars)
