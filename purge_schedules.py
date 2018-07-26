#!/usr/bin/env python
import signal
import ConfigParser, os, getopt
import logging.config
import time
import argparse
from datetime import datetime, timedelta, date
import json
import pymongo
from pymongo import MongoClient
import sys
import re


class DelayedKeyboardInterrupt(object):
    def __enter__(self):
        self.signal_received = False
        self.old_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self.handler)
    def handler(self, signal, frame):
        self.signal_received = (signal, frame)
        logger.debug('SIGINT received. Delaying KeyboardInterrupt.')

    def __exit__(self, type, value, traceback):
        signal.signal(signal.SIGINT, self.old_handler)
        if self.signal_received:
            self.old_handler(*self.signal_recived)
            

# Global
parser = argparse.ArgumentParser(description='Danube Colection Purger')
parser.add_argument('-p', '--propertyfile', help='Property file to be used', required=True)

fmt = '%Y-%m-%d %H:%M:%S'


class MongoDAO(object): 
    def __init__(self, config):
        mongo_uri = config.get('section', 'mongo.uri')
        self.client = MongoClient(mongo_uri)
        db_start_index = mongo_uri.rfind('/') + 1
        db_end_index = mongo_uri.find('?', db_start_index)
        db_str = mongo_uri[db_start_index:] if db_end_index == -1 else mongo_uri[db_start_index:db_end_index]
        db = self.client[db_str]
        self.collection = db[config.get('section', 'purge.collection')]
        
        # naive
        utc_now = datetime.utcnow()
        utc_then = utc_now - timedelta(days=int(config.get('section', 'purge.days.threshold')))
        
        purge_query = config.get('section', 'purge.query')
        purge_query = purge_query.replace('purge.days.threshold', utc_then.strftime(fmt))
        
        self.query = json.loads(purge_query, object_hook=date_hook)
        logger.info('query = %s', self.query)

    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        self.client.Close()
        
    def do_delete(self):        
        return self.collection.find_one_and_delete(self.query)

    def do_find(self):
        logger.debug("Query : " + json.dumps(self.query, default = date_handler))
        e = self.collection.find(self.query).sort("_pub", pymongo.ASCENDING).limit(1).explain()
        logger.debug("Query Plan : " + json.dumps(e, default = date_handler))

        
def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj

def date_hook(json_dict):
    for (key, value) in json_dict.items():
        try:
            if isinstance(value, basestring) and re.search('(\d+-\d+-\d+)', value) is not None:
                json_dict[key] = datetime.strptime(str(value), fmt)
        except AttributeError as e:
            logger.error(e)
            pass
        except:
            logger.error(sys.exc_info()[0])
            pass
    return json_dict            
        

def get_config():
    config = ConfigParser.ConfigParser()
    args = vars(parser.parse_args())
    config.read(args['propertyfile'])
    return config


if __name__ == "__main__":    
    config = get_config();
    log_config_path = config.get('section', 'log.conf.path')
    logging.config.fileConfig(log_config_path)

    global logger
    logger = logging.getLogger('SchedulePurger')

    logger.info('Loading config: %s', config.items('section'))
                
    max_deletes = int(config.get('section', 'max.purge.count'))
    wait_time = float(config.get('section', 'purge.wait.time'))    
    mode = config.get('section', 'purge.run.mode')
         
    do_delete = True if max_deletes == -1 or max_deletes > 0 else False 
    delete_count = 0
    
    mongo_dao = MongoDAO(config)

    logger.info('Starting ...')
    
    while do_delete:
        with DelayedKeyboardInterrupt():
#             mongo_dao.do_find()
            if mode == 'purge':
                result = mongo_dao.do_delete()
                if result is None:
                    logger.info('The end of purging, total count: %d', delete_count)
                    break
                delete_count += 1
                if delete_count % 1000 == 0:
                    logger.info(str(result))
                    logger.info('delete_count so far: %d', delete_count)
                else:
                    logger.debug(str(result))
            else:
                mongo_dao.do_find()
            pass

        if max_deletes > -1 and delete_count >= max_deletes:
            do_delete = False
        
        if do_delete:
            logger.debug('Sleeping for ' + str(wait_time) + ' seconds')
            # TODO : Add back-off support.
            time.sleep(wait_time)
