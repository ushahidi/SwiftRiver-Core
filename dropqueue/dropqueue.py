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

from httplib2 import Http
from httplib import BadStatusLine
from os.path import realpath, dirname
from collections import deque
from threading import Thread, Timer
from Queue import Queue

from swiftriver import Daemon, Worker,  Consumer, Publisher


class DropQueueWorker(Worker):

    def __init__(self, name, job_queue, confirm_queue, meta_publisher,
                 api_url):
        self.meta_publisher = meta_publisher
        self.api_url = api_url
        self.h = Http()
        self.drops = deque()
        self.schedule_posting()
        Worker.__init__(self, name, job_queue, confirm_queue)

    def schedule_posting(self):
        """Schedule processing of the drops deque.
        
        Uses a random timeout between 5 - 15 seconds to introduce some entropy
        and to reduce the chance of workers bombarding the endpoint at the same
        time.
        """
        Timer(5.0 + random.randint(0, 10), self.post_drops).start()

    def work(self):
        """Batch drops in a local in-memory Queue."""
        log.info(" %s started" % self.name)
        while True:
            self.drops.append(self.job_queue.get(True))

    def post_drops(self):
        """POSTS all drops accumulated in a single batch.""" 
        if not len(self.drops):
            self.schedule_posting()
            return

        # Get any drops currently in the deque
        drops = []
        delivery_tags = []
        while True:
            try:
                routing_key, delivery_tag, drop = self.drops.pop()
                drops.append(json.loads(drop))
                delivery_tags.append(delivery_tag)
            except IndexError, msg:
                break

        if not len(drops):
            return

        log.info(" %s posting %d drops to api" % (self.name, len(drops)))
        start_time = time.time()
        resp = content = None
        while not resp:
            try:
                resp, content = self.h.request(self.api_url, 'POST',
                                               body=json.dumps(drops))

                # If no OK response, keep retrying
                if resp.status != 200:
                    log.error("%s NOK response from the API (%d), retrying." 
                              % (self.name, resp.status))
                    resp = content = None
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
        
        # Publish new drops for metadata extraction
        new_drops =json.loads(content)
        if new_drops is not None:
            for drop in new_drops:
                self.meta_publisher.publish(drop)
            
        # Confirm droplet processing complete
        for delivery_tag in delivery_tags:
            self.confirm_queue.put(delivery_tag, False)

        # Reschedule processing of the deque
        self.schedule_posting()
        log.info("%s finished processing in %fs" % (self.name, time.time()-start_time))


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
        
        drop_consumer = Consumer(
            "dropqueue-consumer", self.mq_host,
            Consumer.DROPLET_QUEUE,
            {'durable_queue': True,
             'prefetch_count': self.num_workers * self.batch_size})

        for x in range(self.num_workers):
            DropQueueWorker(
                "dropqueue-worker-" + str(x), drop_consumer.message_queue,
                drop_consumer.confirm_queue, meta_publisher, self.api_url)

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
