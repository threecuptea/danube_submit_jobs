__author__ = 'threecuptea'

import argparse
import ConfigParser
import logging.config
import os
from os.path import dirname, join, isfile
from pymongo import MongoClient
import json
from datetime import datetime, timedelta
import subprocess
import sys

# Need to work with Alon to deploy it to docker


class MongoDao:
    def __init__(self, config):
        mongo_uri = config.get('section', 'mongo.uri')
        self.client = MongoClient(mongo_uri)
        db_start_index = mongo_uri.rfind('/') + 1
        db_end_index = mongo_uri.find('?', db_start_index)
        db_str = mongo_uri[db_start_index:] if db_end_index == -1 else mongo_uri[db_start_index:db_end_index]

        db = self.client[db_str]
        self.collection = db[config.get('section', 'mongo.collection')]
        # {id: {$ne: null}, at: {$lt: ISODate("2018-05-01T04:00:00Z")}}
        utc_now = datetime.utcnow()
        utc_then = utc_now - timedelta(int(config.get('section', 'days.threshold')))
        utc_then_zero = datetime(utc_then.year, utc_then.month, utc_then.day)  # I cannot use utc_then.date()

        self.query_json = json.loads(config.get('section', 'mongo.query')) # Returned dict
        # Replace days.threashold
        dict_replace(self.query_json, 'days.threshold', utc_then_zero)


    def do_cleanup(self):
        self.cursor.close()
        self.client.close()

    def do_get_cursor(self):
        logger.info('query= ' + json.dumps(self.query_json, default=date_handler))
        self.cursor = self.collection.find(self.query_json)
        return self.cursor


def dict_replace(input_dict, repl_from, repl_to):
    for key, value in input_dict.items():
        if type(value) is dict:
            dict_replace(value, repl_from, repl_to)
        elif value == repl_from:
            input_dict[key] = repl_to
            return

def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj

def get_config():
    parser = argparse.ArgumentParser(description='Submit expired UDF airings')
    parser.add_argument('-p', '--properties-file', help='Path of the configuration properties file.')
    args = parser.parse_args()

    if args.properties_file is None:
        print "ERROR: configuration properties file is required to specify to submit expired UDF airing"
        sys.exit(-1)
    if not isfile(args.properties_file):
        print "ERROR: input file '%s' was not found" % args.properties_file
        sys.exit(-1)

    # Read configuration file
    config = ConfigParser.ConfigParser()
    config.read(args.properties_file)
    return config

if __name__ == '__main__':
    # get config from property file
    config = get_config()
    log_config_path = config.get('section', 'log.conf.path')
    logging.config.fileConfig(log_config_path)

    global logger
    logger = logging.getLogger('SubmitLogger')
    logger.info('Loading config: %s', config.items('section'))

    dao = MongoDao(config)

    parent_dir = dirname(__file__)
    dump_file = join(parent_dir, "expired_ids.dump")
    f = open(dump_file, 'w')

    cursor = dao.do_get_cursor()
    logger.info('Output %d ids to expired_ids.dump' % cursor.count())
    for doc in cursor:
        airing_id = doc['_id']
        # Stripe off namespace and resource_type
        schedule_id = airing_id[(airing_id.rfind(')')+1):]
        f.write(schedule_id+"\n")

    f.close()
    dao.do_cleanup()

    # Submit....
    path = config.get('section', 'submit.path')
    command = '%s -r airing -f expired_ids.dump -P 4 &' % (path)
    logger.info('Submitting....., command=  %s' % command)
    FNULL = open(os.devnull, 'w')
    subprocess.call(command, stdout=FNULL, stderr=subprocess.STDOUT, shell=True)
    logger.info('')






















