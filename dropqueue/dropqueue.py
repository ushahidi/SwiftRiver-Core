#!/usr/bin/env python
# encoding: utf-8
"""
Creates drops in the SwiftRiver db by posting drops published to the
DROPLET_QUEUE queue on the MQ to the api endpoint /api/drops and publishes
newly created drops back to the metadata exchange in the MQ for metadata
extraction.

Copyright (c) 2012 Ushahidi. All rights reserved.
"""

import sys
import time
import logging as log
import socket
import json
import ConfigParser
import random
import uuid
import os
from httplib2 import Http
from httplib import BadStatusLine
from os.path import realpath, dirname
from collections import deque
from threading import Thread, Timer, RLock
from Queue import Queue

from swiftriver import Daemon, Worker,  Consumer, Publisher


class DropQueueWorker(Worker):

    def __init__(self, name, job_queue, confirm_queue, meta_publisher, cb_queue, drop_store):
        self.meta_publisher = meta_publisher
        self.drop_store = drop_store
        self.cb_queue = cb_queue
        Worker.__init__(self, name, job_queue, confirm_queue)

    def work(self):
        """Add drop to in memory dict and post for metadata extraction"""
        log.info(" %s started" % self.name)
        while True:
            method, properties, body = self.job_queue.get(True)
            delivery_tag = method.delivery_tag
            corr_id = str(uuid.uuid4())
            drop = json.loads(body)
            drop['source'] = None # Will be populated by metadata workers.

            with self.drop_store['lock']:
                self.drop_store['drops'][corr_id] = {}
                self.drop_store['drops'][corr_id]['drop'] = drop
                self.drop_store['drops'][corr_id]['delivery_tag'] = delivery_tag
            self.meta_publisher.publish(drop, reply_to=self.cb_queue, corr_id=corr_id)
            log.info(" %s Published drop with correlation_id %s for metadata extraction" %
                     (self.name, corr_id))
                     
        
class CallbackWorker(Worker):

   def __init__(self, name, job_queue, confirm_queue, drop_confirm_queue, drop_store, cb_queue, 
                drop_filter_publisher, api_url):
        self.drop_confirm_queue = drop_confirm_queue
        self.drop_store = drop_store
        self.cb_queue = cb_queue
        self.drop_filter_publisher = drop_filter_publisher
        self.api_url = api_url
        self.publish_queue = deque()
        self.schedule_posting()
        self.h = Http()
        Worker.__init__(self, name, job_queue, confirm_queue)

   def work(self):
        method, properties, body = self.job_queue.get(True)
        drop = json.loads(body)
        corr_id = properties.correlation_id
        log.info(" %s drop received from %s, with correlation_id %s" %
                 (self.name, drop['source'], properties.correlation_id))
                 
        with self.drop_store['lock']:
            if corr_id in self.drop_store['drops']:
                for k in drop.keys():
                    self.drop_store['drops'][corr_id]['drop'][k] = drop[k]

                if 'media_complete' in self.drop_store['drops'][corr_id]['drop'] and \
                   'semantics_complete' in self.drop_store['drops'][corr_id]['drop']:
                   log.debug(" %s drop with correlation_id %s has completed metadata extraction" %
                            (self.name, properties.correlation_id))
                
                   # Metadata extraction complete, post to the API                                   
                   delivery_tag = self.drop_store['drops'][corr_id]['delivery_tag']
                   message = self.drop_store['drops'][corr_id]['drop']
                   # We do not send media_complete and semantics complete to the api
                   del message['media_complete']
                   del message['semantics_complete']
                   self.publish_queue.append((delivery_tag, message))
                   del self.drop_store['drops'][corr_id]
                   
        self.confirm_queue.put(method.delivery_tag, False)
                        
   def schedule_posting(self):
       """Schedule processing of the drops deque.
   
       Uses a random timeout between 5 - 15 seconds to introduce some entropy
       and to reduce the chance of workers bombarding the endpoint at the same
       time.
       """
       Timer(5.0 + random.randint(0, 10), self.post_drops).start()
       
   def post_drops(self):
       """POSTS all drops accumulated in a single batch.""" 
       if not len(self.publish_queue):
           self.schedule_posting()
           return
   
       # Get any drops currently in the deque
       drops = []
       delivery_tags = []
       while True:
           try:
               delivery_tag, drop = self.publish_queue.pop()
               drops.append(drop)
               delivery_tags.append(delivery_tag)
           except IndexError, msg:
               break
   
       log.info(" %s posting %d drops to api" % (self.name, len(drops)))
       start_time = time.time()
       resp = None
       while not resp:
           try:
               resp, content = self.h.request(self.api_url, 'POST',
                                              body=json.dumps(drops))
   
               # If no OK response, keep retrying
               if resp.status != 200:
                   log.error("%s NOK response from the API (%d), retrying." 
                             % (self.name, resp.status))
                   time.sleep(60)
           except socket.error, msg:
               log.error("%s Error communicating with api(%s). Retrying" %
                         (self.name, msg))
               time.sleep(60  + random.randint(0, 120)) # Retry after a random delay
           except BadStatusLine, e:
               log.error("%s BadStatusLine Error communicating with api(%s). Retrying" % (self.name, e))
               time.sleep(60  + random.randint(0, 120)) # Retry after a random delay
           except AttributeError, e:
               log.error("%s AttributeError Error communicating with api(%s). Retrying" % (self.name, e))
               time.sleep(60  + random.randint(0, 120)) # Retry after a random delay
   
   
       # Confirm droplet processing complete
       for delivery_tag in delivery_tags:
           self.drop_confirm_queue.put(delivery_tag, False)
   
       # Reschedule processing of the deque
       self.schedule_posting()
       log.info("%s finished processing %d drops in %fs" % (self.name,  len(drops), time.time()-start_time))

class MetaDataPublisher(Publisher):
    
    def __init__(self, mq_host):
        Publisher.__init__(self, "MetaData Publisher", mq_host, 
                           exchange_name='metadata', exchange_type='fanout')


class DropQueueDaemon(Daemon):

    DROPLET_QUEUE = 'DROPLET_QUEUE'

    def __init__(self, num_workers, batch_size, mq_host, api_url,
                 pid_file, out_file):
        Daemon.__init__(self, pid_file, out_file, out_file, out_file)
        
        self.num_workers = num_workers
        self.batch_size = batch_size
        self.api_url = api_url
        self.mq_host = mq_host

    def run(self):
        # Start the metadata publishing thread
        meta_publisher = MetaDataPublisher(self.mq_host)
        
        # In memory drop dict
        drop_store = {'lock': RLock(), 'drops': {}}
        
        cb_queue = 'DROP_CB_QUEUE_' + socket.gethostname() + '-' + str(os.getpid())
        cb_consumer = Consumer(
            "callback-consumer", self.mq_host,
            cb_queue,
            {'exclusive': True,
             'prefetch_count': self.num_workers * self.batch_size})

        drop_consumer = Consumer(
            "dropqueue-consumer", self.mq_host,
            Consumer.DROPLET_QUEUE,
            {'durable_queue': True,
             'prefetch_count': self.num_workers * self.batch_size,
             'exchange_name': 'drops',
             'exchange_type': 'direct'})
             
        drop_filter_publisher = Publisher("Drop Filter Publisher", mq_host, queue_name='DROP_FILTER_QUEUE')
        for x in range(self.num_workers):
            CallbackWorker(
                "callback-worker-" + str(x), cb_consumer.message_queue, 
                cb_consumer.confirm_queue, drop_consumer.confirm_queue, drop_store, cb_queue, 
                drop_filter_publisher, self.api_url)

        for x in range(self.num_workers):
            DropQueueWorker(
                "dropqueue-worker-" + str(x), drop_consumer.message_queue,
                drop_consumer.confirm_queue, meta_publisher, cb_queue, drop_store)

        log.info("Workers started");

        drop_consumer.join()

    def onexit(self):
        log.info("Exiting")


if __name__ == "__main__":
    config = ConfigParser.SafeConfigParser()
    config.readfp(open(dirname(realpath(__file__)) + '/config/dropqueue.cfg'))
    
    try:
        log_file = config.get("main", 'log_file')
        out_file = config.get("main", 'out_file')
        pid_file = config.get("main", 'pid_file')
        num_workers = config.getint("main", 'num_workers')
        batch_size = config.getint("main", 'batch_size')
        log_level = config.get("main", 'log_level')
        api_url = config.get("main", 'api_url')
        mq_host = config.get("main", 'mq_host')

        FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        log.basicConfig(filename=log_file,
                        level=getattr(log, log_level.upper()), format=FORMAT)

        file(out_file, 'a') # Create outfile if it does not exist

        daemon = DropQueueDaemon(num_workers, batch_size, mq_host, api_url,
                                 pid_file, out_file)

        if len(sys.argv) == 2:
            if 'start' == sys.argv[1]:
                daemon.start()
            elif 'stop' == sys.argv[1]:
                daemon.stop()
            elif 'restart' == sys.argv[1]:
                daemon.restart()
            else:
                print "Unknown command"
                sys.exit(2)
            sys.exit(0)
        else:
            print "usage: %s start|stop|restart" % sys.argv[0]
            sys.exit(2)
    except ConfigParser.NoOptionError, e:
        log.error(" Configuration error:  %s" % e)
