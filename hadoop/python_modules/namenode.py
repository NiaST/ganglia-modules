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

HOST = 'localhost'

logging.basicConfig(level=logging.ERROR, format="%(asctime)s - %(name)s - %(levelname)s\t Thread-%(thread)d - %(message)s")
logging.debug('starting up')

# short name to full path for stats
keyToPath = dict()

# JMX METRICS #
keyToPath['nn_dfs_total_space'] = ['FSNamesystemState', 'CapacityTotal']
keyToPath['nn_dfs_used_space'] = ['FSNamesystemState', 'CapacityUsed']
keyToPath['nn_dfs_remaining_space'] = ['FSNamesystemState', 'CapacityRemaining']
keyToPath['nn_dfs_blocks_total'] = ['FSNamesystem', 'BlocksTotal']
keyToPath['nn_dfs_blocks_capacity'] = ['FSNamesystem', 'BlockCapacity']
keyToPath['nn_dfs_files_total'] = ['FSNamesystem', 'FilesTotal']

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
        if met_name.endswith(search):
            return index

    return -1

def get_value(name):
    """Return a value for the requested metric"""

    metrics = update_stats()

    try:
        key_index = int(get_key_index(keyToPath[name][0], metrics))
        key_string = keyToPath[name][1]
        result = metrics['beans'][key_index][key_string]
    except StandardError,e:
        result = 0

    return result

def create_desc(skel, prop):
    d = skel.copy()
    for k, v in prop.iteritems():
        d[k] = v
    return d


def update_stats():
    global HOST
    logging.debug('[namenode] Received the following parameters')

    url_json = 'http://' + HOST + ':50070/jmx'

    # First iteration - Grab statistics
    logging.debug('[namenode] Fetching ' + url_json)
    result = json.load(urllib.urlopen(url_json))
    return result


def metric_init(params):
    descriptors = []
    logging.debug(params)

    metric_group = params.get('metric_group', 'cloudera')

    Desc_Skel = {
        'name': 'XXX',
        'time_max': 60,
        'call_back': get_value,
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
            'name': 'nn_dfs_total_space',
            'units': 'bits',
			'value_type' : 'double',
            'format': '%f',
            'description': 'Configured Capacity (bits)',
        })
    )

    descriptors.append(
        _create_desc({
            'name': 'nn_dfs_used_space',
            'units': 'bits',
			'value_type' : 'double',
            'format': '%f',
            'description': 'DFS Used Space (bits)',
        })
    )
	
    descriptors.append(
        _create_desc({
            'name': 'nn_dfs_remaining_space',
            'units': 'bits',
			'value_type' : 'double',
            'format': '%f',
            'description': 'DFS Remaining Space (bits)',
        })
    )
	

    descriptors.append(
        _create_desc({
            'name': 'nn_dfs_blocks_total',
            'units': 'blocks',
			'value_type' : 'double',
            'format': '%f',
            'description': 'Total Blocks (blocks)',
        })
    )

    descriptors.append(
        _create_desc({
            'name': 'nn_dfs_blocks_capacity',
            'units': 'blocks',
			'value_type' : 'double',
            'format': '%f',
            'description': 'Block Capacity (blocks)',
        })
    )
	
    descriptors.append(
        _create_desc({
            'name': 'nn_dfs_files_total',
            'units': 'files',
			'value_type' : 'double',
            'format': '%f',
            'description': 'Total Files (blocks)',
        })
    )
	
    return descriptors


def metric_cleanup():
    pass


#This code is for debugging and unit testing
if __name__ == '__main__':
    descriptors = metric_init({})
    for d in descriptors:
        v = d['call_back'](d['name'])
        print (('%s = %s') % (d['name'], d['format'])) % v
        logging.debug('value for %s is %s' % (d['name'], str(v)))