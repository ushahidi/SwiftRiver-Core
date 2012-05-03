#!/usr/bin/env python
# encoding: utf-8

import ConfigParser
import json
import logging as log
import socket
import sys
import time
import utils
from os.path import dirname, realpath
from threading import Thread, RLock, Event

import MySQLdb

from swiftriver import Daemon, Worker, Consumer, Publisher


class TwitterFirehoseManager(Daemon):
    """ This class manages updates to the track and follow predicates
    which are submitted to the Twitter Firehose via the streaming API
    
    It has an internal queue that monitors the current no. of predicates
    as per the guidelines specified by Twitter.This class also takes care
    of disconnecting and re-connecting to the Firehose whenever predicates
    are added/deleted
    """
    
    def __init__(self, pid_file, out_file, mq_host, db_config, num_workers):
        Daemon.__init__(self, pid_file, out_file, out_file, out_file)
        
        self.__mq_host = mq_host
        self.__db_config = db_config
        self.__lock = RLock()
        self.__db = None
        
        self.__predicate_workers = num_workers
        
        # Filter predicates for the firehose
        self.__predicates = {}
        
        # Tracks whether the predicates have changed
        self.__predicates_changed = False

    def get_cursor(self):
        """ Returns a cursor object"""
        cursor = None
        while not cursor:
            try:        
                if not self.__db:
                    self.__db = MySQLdb.connect(host=self.__db_config['host'],
                                                port=self.__db_config['port'],
                                                passwd=self.__db_config['pass'], 
                                                user=self.__db_config['user'],
                                                db=self.__db_config['database'])
                                                        
                self.__db.ping(True)
                cursor = self.__db.cursor()
            except MySQLdb.OperationalError, e:
                log.error("%s Error connecting to the database. Retrying..." % e)
                time.sleep(60)
        
        return cursor;
            
    def add_filter_predicate(self, payload):
        """ Adds a filter predicate to the firehose """
        
        # Grab essential data
        predicate_type = self.__get_predicate_type(payload['key'])
        filter_predicate = json.loads(payload['value'])['value']
        river_id = payload['river_id']
            
        # Get the update target
        update_target = self.__predicates.get(predicate_type, dict())
        current_target = dict(update_target)
            
        # Check for filter predicate limits
        if not utils.allow_filter_predicate(self.__predicates, predicate_type):
            log.info("The maximum no. of %s predicates allowed has been reached." 
                     % predicate_type)
            return
            
        # Proceed
        self.__sanitize_filter_predicate(filter_predicate, 
                                         update_target, river_id)
            
        # set-list conversion
        for k, v in update_target.iteritems(): update_target[k] = list(v)
            
        # Update internal list of predicates
        self.__predicates[predicate_type] = update_target
            
        publish_data = {}
        if len(current_target) == 0:
            # Predicate type was non-existent before sanitization, 
            # ignore diff compute
            publish_data[predicate_type] = update_target
        else:
            # Predicate type existing prior to sanitization, compute diff
            new_predicates = list(set(update_target.keys()) - 
                                  set(current_target.keys()))
                
            # Check for new predicates                
            if len(new_predicates) > 0:
                publish_data[predicate_type] = {}
                k = map(lambda x: dict({x:[river_id]}), new_predicates)
                for v in k: publish_data[predicate_type].update(v)
            elif len(new_predicates) == 0:
                # No new filter predicates, check for rivers update for each predicate
                publish_data[predicate_type] = {}
                    
                for k,v in update_target.iteritems():
                    # Get the rivers associated with the current list of rivers
                    g = current_target[k]
                    rivers_diff = list(set(v) - set(g))
                    if len(rivers_diff) > 0:
                        publish_data[predicate_type].update({k: rivers_diff})
                    
                # Verify that there's data to be published
                if len(publish_data[predicate_type]) == 0: publish_data = {}
                
        # Check if there's any data to be published            
        if len(publish_data) > 0:
            log.debug("Publishing new predicates to the firehose %r" 
                      % publish_data)
            self.firehose_publisher.publish(publish_data)    

    def __get_predicate_type(self, payload_key):
        """ Given the payload key, returns the predicate type"""
        
        return "follow" if payload_key.lower() == "person" else "track"
    

    def __sanitize_filter_predicate(self, filter_predicate, update_target, river_id):
        """Given a filter predicate, splits it, removes '#' and '@'
        and pushes it to the specified target"""
        
        for term in filter_predicate.split(","):
            term = term.lower().strip();
            
            # Strip '@' and '#' off the filter predicate
            term = term[1:] if term[:1] == '@' else term
            term = term[1:] if term[:1] == '#' else term

            # As per the streaming API guidelines, terms should be 60 chars
            # long at most
            if  len(term) > 60:
                continue
            
            if not update_target.has_key(term):
                update_target[term] = set()
            else:
                L = set(update_target[term])
                update_target[term] = L

            # Store the river id with that item 
            update_target[term].add(river_id)
            
            
    def delete_filter_predicate(self, payload):
        # Remove the predicate from the internal cache and from 
        # firehose predicates list
        predicate_key =  self.__get_predicate_type(payload['key'])
        filter_predicate = json.loads(payload['value'])['value']
        river_id = payload['river_id']

        delete_items = {}
        self.__sanitize_filter_predicate(filter_predicate, delete_items, river_id)
        
        # Get the current set of predicates from memory 
        current_predicates = self.__predicates.get(predicate_key, dict())
        
        # Nothing to delete
        if len(current_predicates) == 0:
            return
        
        # Delete
        for k, v in delete_items.iteritems():
            # Get the rivers
            rivers = map(lambda x: int(x), current_predicates.get(k, []))
            
            s = map(lambda x: int(x), v)
            
            # Get the delta of the two sets of river ids
            delta = list(set(rivers) - set(s))
            
            if len(delta) == 0:
                # No rivers for that predicate, remove it
                del current_predicates[k]
            else:
                current_predicates[k] = delta
        
        
        log.info("Deleted filter predicate %s from river %s" 
                 % (filter_predicate, river_id))
        
        self.__predicates[predicate_key].update(current_predicates)
        
        # Notify the firehose worker of the change
        log.info("New filter predicates: %r" % json.dumps(self.__predicates))
        self.firehose_publisher.publish({'message': 'replace', 'data':self.__predicates})
    
    def __get_firehose_predicates(self):
        """Gets all the twitter channel options and classifies them
        as track and follow predicates"""
        
        c = self.get_cursor()
        c.execute("""
        SELECT river_id, `key`, `value` 
        FROM channel_filters cf, channel_filter_options cfo 
        WHERE cfo.channel_filter_id = cf.id 
        AND cf.channel = 'twitter'
        AND cf.filter_enabled = 1
        """)
        
        predicates = {}
        for river_id, key, value in c.fetchall():
            predicate_key = self.__get_predicate_type(key)
            
            if not predicates.has_key(predicate_key):
                predicates[predicate_key] = {}
            
            if utils.allow_filter_predicate(predicates, predicate_key):
                update_target = predicates[predicate_key]
                
                # Get filter predicate and submit it for sanitization 
                filter_predicate = json.loads(value)['value']
                self.__sanitize_filter_predicate(filter_predicate, 
                                                 update_target, river_id)
                
                predicates[predicate_key] = update_target
            else:
                break
        
        c.close()
        
        # Convert the sets to lists
        for k,v in predicates.iteritems():
            for term, river_ids in v.iteritems():
                predicates[k][term] = list(river_ids) 
        
        return predicates
    
    def run_manager(self):
        # Options to be passed to the predicate update workers
        while True:
            if self.__predicates_changed:
                log.info("Placing filter predicates in the firehose queue %r" 
                         % self.__predicates)
                self.firehose_publisher.publish(self.__predicates)
                self.__predicates_changed = False                
            time.sleep(5)
                
    
    def run(self):
        log.info("Twitter Firehose Manager started")
        
        self.firehose_publisher = FirehosePublisher(self.__mq_host)
        
        # Spawn a set of workers to listen for predicate updates
        options = {'exchange_name': 'chatter', 
                   'exchange_type': 'topic', 
                   'routing_key': 'web.channel_option.twitter.*',
                   'durable_exchange':  True,
                   'prefetch_count': self.__predicate_workers}
        channel_update_consumer = Consumer("channel-update-consumer", 
                                           self.__mq_host, 
                                           utils.TWITTER_UPDATE_QUEUE,
                                           options)
        
        for x in range(self.__predicate_workers):
            TwitterPredicateUpdateWorker("twitter-predicate-updater-" + str(x),
                                         channel_update_consumer.message_queue,
                                         channel_update_consumer.confirm_queue,
                                         self)
            
        # Get all the predicates from the database
        self.__predicates = self.__get_firehose_predicates()
        self.__predicates_changed = (len(self.__predicates) > 0)
        
        self.run_manager()
        
class FirehosePublisher(Publisher):

    def __init__(self, mq_host):
        Publisher.__init__(self, "Firehose Publisher", mq_host, 
                           queue_name=utils.FIREHOSE_QUEUE, durable=False)

class TwitterPredicateUpdateWorker(Worker):
    """
    Listens for add/delete predicate messages originating the web application 
    and updates the internal database/cache
    """
        
    def __init__(self, name, job_queue, confirm_queue, manager):
        self.manager = manager
        Worker.__init__(self, name, job_queue, confirm_queue)
        
    def work(self):
        try:
            routing_key, delivery_tag, body = self.job_queue.get(True)
            message = json.loads(body)
            # Add predicates
            if routing_key == "web.channel_option.twitter.add":
                log.info("Add new twitter predicate...")
                self.manager.add_filter_predicate(message)
            
            # Delete predicates
            if routing_key == "web.channel_option.twitter.delete":
                log.info("Deleting twitter predicate...")
                self.manager.delete_filter_predicate(message)
            
            self.confirm_queue.put(delivery_tag, False)  
        except Exception, e:
            log.info(e)
            log.exception(e)               

if __name__ == '__main__':
    # Load the configuration file
    config = ConfigParser.SafeConfigParser()
    config.readfp(open(dirname(realpath(__file__))+'/config/manager.cfg'))
    
    try:
        # Setup logging
        log_file = config.get('main', 'log_file')
        out_file = config.get('main', 'out_file')
        log_level = config.get('main', 'log_level')
        
        FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        log.basicConfig(filename=log_file, level=getattr(log, 
                                                         log_level.upper()), 
                                                         format=FORMAT)
        
        # Create outfile if it does not exist        
        file(out_file, 'a')

        # Setup the daemon
        pidfile = config.get('main', 'pid_file')
        stdout = config.get('main', 'out_file')
        
        # Get the database config
        db_config = {
            'host': config.get("db", 'host'),
            'port': config.getint("db", 'port'),
            'user': config.get("db", 'user'),
            'pass': config.get("db", 'pass'),
            'database': config.get("db", 'database')
        }
        
        # Daemon options
        mq_host = config.get('main', 'mq_host')
        num_workers = config.getint('main', 'num_workers')

        # Initialize the daemon
        daemon = TwitterFirehoseManager(pidfile, stdout, mq_host, db_config, num_workers)
        
        if len(sys.argv) == 2:
            if sys.argv[1] == 'start':
                daemon.start()
            elif sys.argv[1] == 'stop':
                daemon.stop()
            elif sys.argv[1] == 'restart':
                daemon.restart()
            else:
                print "Unknown command"
                sys.exit(2)
                
            sys.exit(0)
        else:
            print "usage %s start|stop|restart" % sys.argv[1]
            sys.exit(2)
    except ConfigParser.NoOptionError, e:
        log.error(" Configuration error:  %s" % e)