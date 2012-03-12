#!/usr/bin/env python
# encoding: utf-8

import ConfigParser
import json
import logging as log
import MySQLdb
import socket
import sys
import pika
import time
import utils
from os.path import dirname, realpath
from threading import Thread, RLock, Event
from swiftriver import Daemon, Worker


class TwitterFirehoseManager:
    """ This class manages updates to the track and follow predicates
    which are submitted to the Twitter Firehose via the streaming API
    
    It has an internal queue that monitors the current no. of predicates
    as per the guidelines specified by Twitter.This class also takes care
    of disconnecting and re-connecting to the Firehose whenever predicates
    are added/deleted
    """
    
    def __init__(self, mq_host, db_config, predicate_workers):
        self.__mq_host = mq_host
        self.__db_config = db_config
        self.__lock = RLock()
        self.__db = None
        
        self.__predicate_workers = predicate_workers
        
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
            except MySQLdb.OperationalError:
                log.error(" error connecting to db, retrying")
                time.sleep(60)
        
        return cursor;
            
    def add_filter_predicate(self, mq, payload):
        """ Adds a filter predicate to the firehose """
        
        # Acquire lock and proceed
        with self.__lock:
            # Grab essential data
            predicate_type = "follow" if payload['key'].lower() == "person" else "track"
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
            self.__sanitize_filter_predicate(filter_predicate, update_target, river_id)
            
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
                log.debug("Publishing %r to the firehose" % publish_data)
                
                # Construct the message to sent out to the process consuming the firehose
                message = json.dumps(publish_data)
                        
                # Publish the new predicate to the Firehose
                channel = mq.channel()
                channel.queue_declare(queue=utils.FIREHOSE_QUEUE, durable=False)
                channel.basic_publish(exchange = '', 
                                      routing_key=utils.FIREHOSE_QUEUE,
                                      body=message)


    def __sanitize_filter_predicate(self, filter_predicate, update_target, river_id):
        """Given a filter predicate, splits it, removes '#' and '@'
        and pushes it to the specified target"""
        
        for term in filter_predicate.split(","):
            term = term.strip();
            
            # As per the streaming API guidelines, terms should be 60 chars
            # long at most
            if  len(term) > 60:
                continue
            
            # Remove '#' and '@' from the filter predicates
            term = term[1:] if term[:1] == '@' else term
            term = term[1:] if term[:1] == '#' else term
            
            if not update_target.has_key(term):
                update_target[term] = set()
                
            # Store the river id with that item 
            update_target[term].add(river_id)
            
            
    def delete_filter_predicate(self, payload):
        # Remove the predicate from the internal cache and from 
        # firehose predicates list
        pass

    
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
            predicate_key = 'track' if key.lower() == 'keyword' else 'follow'
            
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
    
    def _run(self):
        # Options to be passed to the predicate update workers
        while True:
            try:
                if self.__predicates_changed:
                    message = json.dumps(self.__predicates)
                    log.info("Placing filter predicates in the firehose queue %r" 
                             % message)
                    
                    params = pika.ConnectionParameters(host=self.__mq_host)
                    connection = pika.BlockingConnection(params)
                    
                    channel = connection.channel()
                    channel.queue_declare(queue=utils.FIREHOSE_QUEUE, durable=False)
                    channel.basic_publish(exchange='', 
                                          routing_key=utils.FIREHOSE_QUEUE,
                                          body=message)
                    
                    self.__predicates_changed = False
                
                time.sleep(5)
            except socket.error, msg:
                log.error("%s Firehose manager error connecting to the MQ, retrying" 
                          % msg)
                time.sleep(60)
            except pika.exceptions.AMQPConnectionError, e:
                log.error(" Firehose manager lost connection to the MQ, reconnecting")
                time.sleep(60)
            except pika.exceptions.ChannelClosed, e:
                log.error(" Firehose manager lost connection to the MQ, reconnecting")
                time.sleep(60)
                
    
    def start(self):
        # Spawn a set of workers to listen for predicate updates
        worker_options = {'exchange_name': 'chatter', 
                          'exchange_type': 'topic', 
                          'routing_key': 'web.channel_option.twitter.*'
        }
        for x in range(self.__predicate_workers):
            # Generate the worker name
            worker_name = "twitter-predicate-updater-" + str(x)
                
            # Spawn the worker
            worker = TwitterPredicateUpdateWorker(worker_name, self.__mq_host, 
                                         utils.TWITTER_UPDATE_QUEUE, 
                                         worker_options)
            worker.set_firehose_manager(self)
            worker.start()
            
        # Get all the predicates from the database
        self.__predicates = self.__get_firehose_predicates()
        self.__predicates_changed = (len(self.__predicates) > 0)
        
        # Run the manager
        log.info("Starting the Twitter Firehose Manager thread...")
        Thread(target=self._run).start()


class TwitterPredicateUpdateWorker(Worker):
    """
    Listens for add/delete predicate messages originating the web application 
    and updates the internal database/cache
    """
        
    def __init__(self, name, mq_host, queue, options=None):
        Worker.__init__(self, name, mq_host, queue, options)
        self.__firehose_manager = None
    
    def set_firehose_manager(self, manager):
        """Sets the firehose manager reference"""
        self.__firehose_manager = manager
        
    def handle_mq_response(self, ch, method, properties, body):
        try:
            payload = json.loads(body)
            # Add predicates
            if method.routing_key == "web.channel_option.twitter.add":
                self.__firehose_manager.add_filter_predicate(self.mq, payload)
            
            # Delete predicates
            if method.routing_key == "web.channel_option.twitter.delete":
                log.info("Firehose filter predicate removed %s" % payload['value'])
                self.__firehose_manager.delete_filter_predicate(payload)
                
        except Exception, e:
            log.info(e)
            log.exception(e)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    
class TwitterFirehoseManagerDaemon(Daemon):
    
    def __init__(self, pidfile, stdout, options):
        Daemon.__init__(self, pidfile, stdout, stdout, stdout)
        
        # TODO: Raise exception if the options are not specified
#        if options is None:
#            raise Error
        self.__mq_host = options.get('mq_host')
        self.__db_config = options.get('db_config')
        self.__num_workers = options.get('num_workers')
        
    def run(self):
        # Initialize the firehose manager
        try:
            event = Event()
            TwitterFirehoseManager(self.__mq_host, self.__db_config, 
                                   self.__num_workers).start()
            event.wait()
        except Exception, e:
            log.exception(e)
        finally:
            log.info("Exiting...")
       


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
        options = {'mq_host': config.get('main', 'mq_host'),
                   'num_workers': config.getint('main', 'num_workers'),
                   'db_config': db_config
                   }

        # Initialize the daemon
        daemon = TwitterFirehoseManagerDaemon(pidfile, stdout, options)
        
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