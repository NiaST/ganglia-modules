#! /usr/bin/python

try:
    import simplejson as json
    assert json  # silence pyflakes
except ImportError:
    import json

import logging
import time
import urllib
import re
from functools import partial

last_update = 0
cur_time = 0
HOST = 'localhost'
stats = {}
last_val = {}
MAX_UPDATE_TIME = 15

logging.basicConfig(level=logging.ERROR, format="%(asctime)s - %(name)s - %(levelname)s\t Thread-%(thread)d - %(message)s")
logging.debug('starting up')

# short name to full path for stats
keyToPath = dict()

# JMX METRICS #
keyToPath['dn_dfs_blocks_read'] = ['DataNodeActivity', 'BlocksRead']
keyToPath['dn_dfs_blocks_removed'] = ['DataNodeActivity', 'BlocksRemoved']
keyToPath['dn_dfs_blocks_replicated'] = ['DataNodeActivity', 'BlocksReplicated']
keyToPath['dn_dfs_blocks_verified'] = ['DataNodeActivity', 'BlocksVerified']
keyToPath['dn_dfs_blocks_written'] = ['DataNodeActivity', 'BlocksWritten']

def dig_it_up(obj, path):
    try:
        if type(path) in (str, unicode):
            path = path.split('.')
        return reduce(lambda x, y: x[y], path, obj)
    except:
        return False

def get_key_index(search, metrics):
    """Return a value for the requested metric"""
    nbr_elements = len(metrics['beans'])
    for index in range(len(metrics['beans'])):
        met_name = metrics['beans'][index]['name']
        if met_name.find(search) != -1:
            #print 'index = ' + str(index)
            return index

    return -1

def get_value(name):
    """Return a value for the requested metric"""
    global stats

    metrics = update_stats()
    if metrics:
        key_string = keyToPath[name][1]
        return stats[key_string]
    else:
        return 0


def get_delta(key, val, convert=1):
	logging.debug(' get_delta for ' + key)
	global stats, last_val

	if convert == 0:
		logging.warning(' convert is zero!')

	interval = cur_time - last_update
	#print 'interval = ' + str(interval)
	#print 'cur_time = ' + str(cur_time)
	#print 'last_update 2 = ' + str(last_update)
	#print 'key in list = ' + str(key in last_val)
	if key in last_val and interval > 0:
		#print 'key = ' + key + ' | curr val = ' + str(val) + ' | last val = ' + str(last_val[key])
		stats[key] = (val - last_val[key]) * float(convert) / float(interval)
		#print ' => final val = ' + str(stats[key])
	else:
		stats[key] = 0

	last_val[key] = float(val)


def create_desc(skel, prop):
    d = skel.copy()
    for k, v in prop.iteritems():
        d[k] = v
    return d


def update_stats():
    global HOST, MAX_UPDATE_TIME
    global last_update, stats, last_val, cur_time

    cur_time = time.time()

    url_json = 'http://' + HOST + ':50075/jmx'

    # First iteration - Grab statistics
    logging.debug('[datanode] Fetching ' + url_json)
    metrics = json.load(urllib.urlopen(url_json))

    if cur_time - last_update < MAX_UPDATE_TIME:
        logging.debug(' wait ' + str(int(MAX_UPDATE_TIME - (cur_time - last_update))) + ' seconds')
        return True

    try:
        for itm in keyToPath:
            metric_string = keyToPath[itm][0]
            key_string = keyToPath[itm][1]
            key_index = int(get_key_index(metric_string, metrics))
            result = metrics['beans'][key_index][key_string]
            #print 'result = ' + str(result)
            get_delta(key_string, float(result) )
            last_val[key_string] = result
    except StandardError,e:
        #print e
        result = 0
	
    last_update = cur_time
    return True


def metric_init(params):
    descriptors = []
    logging.debug('[datanode] Received the following parameters')
    logging.debug(params)

    metric_group = params.get('metric_group', 'cloudera')

    Desc_Skel = {
        'name': 'XXX',
        'call_back': get_value,
        'time_max': 60,
        'value_type': 'uint',
        'units': 'units',
        'slope': 'both',
        'format': '%d',
        'description': 'XXX',
        'groups': metric_group,
    }

    _create_desc = partial(create_desc, Desc_Skel)

    descriptors.append(
        _create_desc({
            'name': 'dn_dfs_blocks_read',
            'units': 'blocks/s',
            'value_type': 'float',
            'format': '%f',
            'description': 'Number of times that a block is read from the hard disk (blocks)',
        })
    )

    descriptors.append(
        _create_desc({
            'name': 'dn_dfs_blocks_removed',
            'units': 'blocks/s',
            'value_type': 'float',
            'format': '%f',
            'description': 'Number of removed or invalidated blocks on the DataNode. (blocks)',
        })
    )
	
    descriptors.append(
        _create_desc({
            'name': 'dn_dfs_blocks_replicated',
            'units': 'blocks/s',
            'value_type': 'float',
            'format': '%f',
            'description': 'Number of blocks transferred or replicated from one DataNode to another. (blocks)',
        })
    )

    descriptors.append(
        _create_desc({
            'name': 'dn_dfs_blocks_verified',
            'units': 'blocks/s',
            'value_type': 'float',
            'format': '%f',
            'description': 'Number of block verifications (blocks)',
        })
    )

    descriptors.append(
        _create_desc({
            'name': 'dn_dfs_blocks_written',
            'units': 'blocks/s',
            'value_type': 'float',
            'format': '%f',
            'description': 'Number of blocks written to disk (blocks)',
        })
    )
	
    return descriptors


def metric_cleanup():
    pass


#This code is for debugging and unit testing
if __name__ == '__main__':
    descriptors = metric_init({})
    while True:
        for d in descriptors:
            v = d['call_back'](d['name'])
            print (('%s = %s') % (d['name'], d['format'])) % v
            logging.debug('value for %s is %s' % (d['name'], str(v)))
        time.sleep(60)
