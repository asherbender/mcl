"""Tools for handling logged network data.

The network dump tools module provides methods and objects designed to simplify
loading and handling logged network data.

.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
import os
import csv
import numpy as np
import collections
from mcl.logging.file import ReadFile
from mcl.logging.file import ReadDirectory


def dump_to_list(source, min_time=None, max_time=None, message=False):
    """Load message dumps into a list.

    The :py:func:`.dump_to_list` function parses a network dump file or
    directory of network dump files into a list. Note, the following fields are
    added to each object:

        - ``elapsed_time``  the time when the message was logged to file, in
                           seconds, relative to when logging started.
        - ``topic`` the topic that was associated with the message during
                    transmission.

    If the ``elapsed_time`` or ``topic`` exist as keys in the stored object,
    the original data is preserved and a warning message is printed to the
    screen.

    Args:
        source (str): Path of dump file(s) to convert into a list. Path can
                      point to a single file or a directory containing multiple
                      log files.
        min_time (float): Minimum time to extract from dataset.
        max_time (float): Maximum time to extract from dataset.
        message (bool): If set to ``True`` messages will automatically
                        be decoded into the MCL message type stored in the log
                        file. If set to ``False`` (default), message data is
                        returned as a dictionary. Note: to read data as MCL
                        messages, the messages must be loaded into the
                        namespace.

    Returns:
        list: A list of chronologically ordered network messages. The type of
              each item in the list depends on the ``message`` input.

    """

    # Create object for reading a directory of network dumps in time order.
    try:
        if os.path.isdir(source):
            dumps = ReadDirectory(source,
                                  min_time=min_time,
                                  max_time=max_time,
                                  message=message)
        else:
            dumps = ReadFile(source,
                             min_time=min_time,
                             max_time=max_time,
                             message=message)
    except:
        raise

    # Create warning message for fields which exist in the dictionary and might
    # get clobbered.
    warning_msg = "%s: Already contains the key '%s'. "
    warning_msg += "Preserving the original data."

    # Read data from files.
    messages = list()
    time_origin = None
    while True:

        # Parse line from file(s) as a dictionary in the following format:
        #
        #    {'elapsed_time': float(),
        #     'topic': str(),
        #     'message': <:py:class:`.Message`>}
        #
        message = dumps.read()

        # Write message to file.
        if message:
            try:
                topic = message['topic']
                message = message['message']

                if not time_origin:
                    time_origin = message['timestamp']
                    elapsed_time = 0.0
                else:
                    elapsed_time = message['timestamp'] - time_origin

                # Add elapsed time and topic.
                if 'elapsed_time' not in message:
                    message['elapsed_time'] = elapsed_time
                else:
                    print warning_msg % (message['name'], 'elapsed_time')

                # Add message topic.
                if 'topic' not in message:
                    message['topic'] = topic
                else:
                    print warning_msg % (message['name'], 'topic')

                # Store formatted message object in list.
                messages.append(message)
                message = None

            except:
                print '\nCould not convert:'
                print message
                raise

        else:
            break

    return messages


def dump_to_array(source, keys, min_time=None, max_time=None):
    """Load message dump into a numpy array.

    The :py:func:`.dump_to_array` function parses a network dump file or
    directory of network dump files into a numpy array. To parse data into a
    numpy array, the following conditions must be met:

        - All messages loaded must be the same MCL :py:class:`.Message` type.
        - The logged messages must contain the specified keys.
        - The contents of the message keys must be convertible to a float.

    Args:
        source (str): Path to dump file to be formatted into a numpy array. If
                      the dump files are split, provide the prefix to the dump
                      files.
        keys (list): List of message attributes to load into numpy array. The
                     items in this list specify what is copied into the numpy
                     columns.
        min_time (float): Minimum time to extract from dataset.
        max_time (float): Maximum time to extract from dataset.

    Returns:
        numpy.array: A 2D numpy array containing the requested keys (columns)
                     from each message in the dump (rows).

    Raises:
        IOError: If the input `source` does not exist.
        TypeError: If the input `message` is not a string.

    """

    # Default formatting of keys is a list of strings.
    if isinstance(keys, basestring):
        keys = [keys, ]
    elif not isinstance(keys, collections.Iterable):
        raise TypeError("'keys' must be a list of strings.")

    # Load message dumps into a list.
    try:
        message_list = dump_to_list(source,
                                    min_time=min_time,
                                    max_time=max_time)
    except:
        raise

    # Pre-allocate memory for array.
    rows = len(message_list)
    cols = len(keys)
    array = np.zeros((rows, cols))

    # Return nothing if there is no data in range.
    if rows == 0:
        return None

    # Ensure all keys exist in the list before proceeding.
    for key in keys:
        if key not in message_list[0]:
            msg = "The key '%s' does not exist in the message objects "
            msg += "stored in '%s'."
            raise Exception(msg % (key, source))

    # Ensure all messages are the same object.
    for message in message_list:
        if 'name' not in message or message['name'] != message_list[0]['name']:
            msg = "Found a '%s' message object. "
            msg += "Expected all message objects to be '%s' messages."
            raise Exception(msg % (message['name'], message_list[0]['name']))

    # Copy message fields into array.
    for (i, message) in enumerate(message_list):
        try:
            row = np.array([float(message[key]) for key in keys])
            array[i, :] = row
        except:
            array[i, :] = np.NaN
            msg = '\nCould not convert keys in the message:'
            msg += '\n\n%s\n\n'
            msg += 'into an array. Row %i set to NaN.'
            print msg % (str(message), i)

    return array


def dump_to_csv(source, csvfile, keys, min_time=None, max_time=None):
    """Write fields of message dump into a CSV file.

    Args:
        source (str): Path to dump file to be formatted into a CSV file. If the
                      dump files are split, provide the prefix to the dump
                      files.
        csvfile (str): Path to write CSV file.
        keys (list): List of message attributes to load into columns of the CSV
            file.
        min_time (float): Minimum time to extract from dataset.
        max_time (float): Maximum time to extract from dataset.

    """

    # Default formatting of keys is a list of strings.
    if isinstance(keys, basestring):
        keys = [keys, ]
    elif not isinstance(keys, collections.Iterable):
        raise TypeError("'keys' must be a list of strings.")

    # Load message dumps into a list.
    try:
        message_list = dump_to_list(source,
                                    min_time=min_time,
                                    max_time=max_time)
    except:
        raise

    # Return nothing if there is no data in range.
    if len(message_list) == 0:
        return None

    # Ensure all keys exist in the list before proceeding.
    for key in keys:
        if key not in message_list[0]:
            msg = "The key '%s' does not exist in the message objects "
            msg += "stored in '%s'."
            raise Exception(msg % (key, source))

    # Ensure all messages are the same object.
    for message in message_list:
        if 'name' not in message or message['name'] != message_list[0]['name']:
            msg = "Found a '%s' message object. "
            msg += "Expected all message objects to be '%s' messages."
            raise Exception(msg % (message['name'], message_list[0]['name']))

    # Copy message fields into array.
    with open(csvfile, 'wb') as f:
        csv_writer = csv.writer(f)
        for message in message_list:
            try:
                csv_writer.writerow([message[key] for key in keys])
            except:
                msg = 'Could not convert keys in the message:'
                msg += '\n\n%s\n\n'
                msg += 'into an array.'
                raise Exception(msg % str(message))