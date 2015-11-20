import os
import time
import pickle
import numpy as np
from bz2 import BZ2File
from collections import OrderedDict


def ping_pong_stats(payload, pings, pongs, verbose=False):
    """Calculate statistics in ping/pong data.

    Args:
      payload (int): size of payload.
      pings (dict): ping dictionaries.
      pongs (dict): ping dictionaries.

    Returns:
      OrderedDict: summary of ping-pong data

    """

    # Get observed counters (approximate ground truth).
    counters = 0
    for ping_PID in pings.iterkeys():
        counters = max(counters, len(pings[ping_PID]))
        for pong_PID in pongs[ping_PID].iterkeys():
            counters = max(counters, len(pongs[ping_PID][pong_PID]))

    counters = range(counters)
    ping_pongs = len(counters)

    # Get all time stamps.
    times = list()
    for ping_PID in pings.iterkeys():
        t = pings[ping_PID].values()
        times.append(min(t))
        times.append(max(t))
        for pong_PID in pongs[ping_PID].iterkeys():
            t = pongs[ping_PID][pong_PID].values()
            times.append(min(t))
            times.append(max(t))

    # Calculate duration (approximate ground truth).
    min_time = min(times)
    max_time = max(times)
    duration = max_time - min_time

    # Pre-allocate memory for total statistics.
    total_stats = OrderedDict()
    total_stats['ping_PIDs'] = list()
    total_stats['pong_PIDs'] = list()
    total_stats['duration'] = duration
    total_stats['ping_pongs'] = ping_pongs
    total_stats['number_pings'] = 0
    total_stats['number_pongs'] = 0
    total_stats['pings_dropped'] = 0
    total_stats['pongs_dropped'] = 0
    total_stats['ping_transfer_percent'] = 0
    total_stats['pong_transfer_percent'] = 0
    total_stats['ping_data_transferred'] = 0
    total_stats['pong_data_transferred'] = 0
    total_stats['ping_data_rate'] = 0
    total_stats['pong_data_rate'] = 0
    total_stats['mean_latency'] = 0

    # Iterate through ping processes.
    latencies = list()
    for ping_PID in pings.iterkeys():
        total_stats['ping_PIDs'].append(ping_PID)

        # Accumulate total ping statistics.
        number_pings = len(pings[ping_PID])
        ping_counters = set(pings[ping_PID].keys())
        ping_data_transferred = float(payload) * number_pings / 1000000.0
        ping_data_rate = ping_data_transferred / duration
        total_stats['number_pings'] += number_pings
        total_stats['ping_data_transferred'] += ping_data_transferred
        total_stats['ping_data_rate'] += ping_data_rate

        # Iterate through pong processes.
        for pong_PID in pongs[ping_PID].iterkeys():
            total_stats['pong_PIDs'].append(pong_PID)

            # Accumulate total pong statistics.
            number_pongs = len(pongs[ping_PID][pong_PID])
            pong_counters = set(pongs[ping_PID][pong_PID].keys())
            pong_data_transferred = float(payload) * number_pongs / 1000000.0
            pong_data_rate = pong_data_transferred / duration
            total_stats['number_pongs'] += number_pongs
            total_stats['pong_data_transferred'] += pong_data_transferred
            total_stats['pong_data_rate'] += pong_data_rate

            # Iterate through received messages.
            for counter in ping_counters & pong_counters:

                # Store ping/pong time.
                ping = pings[ping_PID][counter]
                pong = pongs[ping_PID][pong_PID][counter]

                # Calculate latency.
                latency = pong - ping
                latencies.append(latency)

    # Average total statistics over processes.
    servers = len(total_stats['ping_PIDs'])
    clients = len(total_stats['pong_PIDs'])
    expected_pings = servers * ping_pongs
    expected_pongs = servers * clients * ping_pongs
    pings_dropped = expected_pings - total_stats['number_pings']
    pongs_dropped = expected_pongs - total_stats['number_pongs']
    ping_transfer_percent = 100.0 * float(total_stats['number_pings']) / float(expected_pings)
    pong_transfer_percent = 100.0 * float(total_stats['number_pongs']) / float(expected_pongs)
    total_transfer_percent = 100.0 * float(total_stats['number_pings'] + total_stats['number_pongs']) / float(expected_pings + expected_pongs)

    # Store total statistics.
    total_stats['pings_dropped'] = pings_dropped
    total_stats['pongs_dropped'] = pongs_dropped
    total_stats['ping_transfer_percent'] = ping_transfer_percent
    total_stats['pong_transfer_percent'] = pong_transfer_percent
    total_stats['total_transfer_percent'] = total_transfer_percent
    total_stats['mean_latency'] = np.mean(latencies)

    return total_stats


def ping_pong_stats_to_str(stats):
    """Print ping/pong statistics.

    Args:
      stats (dict): ping-pong statistics.

    Returns:
      str: human readable ping-pong statistics.

    """

    s = 'duration:                %1.2f seconds\n' % stats['duration']
    s += 'number of pings:         %i\n' % stats['number_pings']
    s += 'number of pongs:         %i\n' % stats['number_pongs']
    s += 'number of ping-pongs:    %i\n' % stats['ping_pongs']
    s += 'number of pings dropped: %i\n' % stats['pings_dropped']
    s += 'number of pongs dropped: %i\n' % stats['pongs_dropped']
    s += 'pings transferred:       %1.1f%%\n' % stats['ping_transfer_percent']
    s += 'pongs transferred:       %1.1f%%\n' % stats['pong_transfer_percent']
    s += 'total transferred:       %1.1f%%\n' % stats['total_transfer_percent']
    s += 'ping data transferred:   %1.2f MB\n' % stats['ping_data_transferred']
    s += 'pong data transferred:   %1.2f MB\n' % stats['pong_data_transferred']
    s += 'ping data rate:          %1.2f MB/s\n' % stats['ping_data_rate']
    s += 'pong data rate:          %1.2f MB/s\n' % stats['pong_data_rate']
    s += 'mean latency:            %1.4f s' % stats['mean_latency']

    return s


def update_cache(root, payloads, clients, transports, rates, force=False,
                 verbose=True):

    # Function for creating file names.
    def create_fname(*args):
        return'%s_%i_%i_%1.3f.pkl' % args

    # Function for creating an empty dictionary if it does not exist.
    def if_key(dct, key):
        if key not in dct:
            dct[key] = OrderedDict()
        return dct

    # Create name to cache.
    cache_path = os.path.join(root, 'cache.pkl')

    # Load cache if it exists.
    if os.path.isfile(cache_path):
        with BZ2File(cache_path, 'r') as f:
            cache = pickle.load(f)

    # Create new cache if it does not exist.
    else:
        cache = dict()
        cache['files'] = dict()

    # Iterate through packet sizes.
    for payload in payloads:
        cache = if_key(cache, payload)

        # Iterate through number of clients.
        for client in clients:
            cache[payload] = if_key(cache[payload], client)

            # Iterate through different transports.
            for trans in transports:
                cache[payload][client] = if_key(cache[payload][client], trans)

                # Iterate through attempted transmission rates..
                for rate in rates:
                    time.sleep(0.05)

                    # Create path to ping-pong data.
                    fname = create_fname(trans.lower(), payload, client, rate)
                    fpath = os.path.join(root, fname)

                    # Create human readable name for configuration.
                    config = '%i, clients: %i, transport: %s, rate: %s'
                    config = config % (payload, client, trans, rate)

                    # Get properties of data file.
                    if os.path.exists(fpath):
                        fstat = os.stat(fpath)
                        fstat = (fstat.st_size, fstat.st_ctime)
                    else:
                        msg = 'File not found %s'
                        msg = msg % (payload, client, trans, rate)
                        print msg
                        continue

                    # Check if data file has changed since the cache was last
                    # updated.
                    if fname in cache['files'] and not force:
                        if cache['files'][fname] == fstat:
                            if verbose:
                                print 'Cached  packet %s' % config
                            continue

                    if verbose:
                        print 'Loading packet %s' % config

                    # Load data.
                    try:
                        with BZ2File(fpath, 'r') as f:
                            data = pickle.load(f)
                            ping_data = data['pings']
                            pong_data = data['pongs']
                    except:
                        print 'Could not load %s' % config
                        continue

                    # Calculate ping/pong stats.
                    stats = ping_pong_stats(payload,
                                            ping_data,
                                            pong_data)

                    # Save statistics.
                    cache[payload][client][trans][rate] = stats
                    cache['files'][fname] = fstat

                    # Write changes to disk.
                    with BZ2File(cache_path, 'w') as f:
                        pickle.dump(cache, f, protocol=2)

    if verbose:
        print 'Done'
    return cache
