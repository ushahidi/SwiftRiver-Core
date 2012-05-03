#!/usr/bin/env python
# encoding: utf-8
"""
Extracts links from the droplets posted to the metadata fanout exchange and publishes
the update droplet back to the DROPLET_QUEUE for updating in the db

Copyright (c) 2012 Ushahidi. All rights reserved.
"""

import sys
import ConfigParser
import logging as log
import pika
import json, re
from threading import Thread
from httplib2 import Http
from os.path import dirname, realpath
from swiftriver import Daemon, Consumer, Worker, DropPublisher


class LinkExtractorQueueWorker(Worker):
    
    def __init__(self, name, job_queue, confirm_queue, drop_publisher):
        self.drop_publisher = drop_publisher
        Worker.__init__(self, name, job_queue, confirm_queue)
    
    def work(self):
        """POSTs the droplet to the semantics API"""
        routing_key, delivery_tag, body = self.job_queue.get(True)
        droplet = json.loads(body)            
        log.info(" %s droplet received with id %d" % (self.name, droplet.get('id', 0)))
        
        # Strip tags leaving only hyperlinks
        droplet_raw = re.sub(r'<(?!\s*[aA]\s*)[^>]*?>', '', droplet['droplet_raw']).strip().encode('ascii', 'ignore')
        
        # Extract the href from hyperlinks since the regex above expects a space character
        # at the end of the url
        droplet_raw = re.sub(r'(?i)<(?=\s*[a]\s+)[^>]*href\s*=\s*"([^"]*)"[^>]*?>', ' \\1 ', droplet_raw)
        
        for link in re.findall("(?:https?://[^\\s]+)", droplet_raw):
            if not droplet.has_key('links'):
                droplet['links'] = []

            if link[:4] != 'http':
                link = 'http://' + link
            
            m = re.search('https?://([^/]+)', link)
            domain = ''
            if m:
                domain = m.group(1)
            
            # Get the full URL but only do so if the link 
            # looks like a shortened url
            if len(link) < 25 and len(domain) < 10:
                log.debug(" %s expanding url %s" % (self.name, link))
                h  = Http()
                try:
                    resp, content = h.request(link, 'HEAD')
                    link = resp.get('content-location', link)
                except Exception, e:
                    log.error(" %s error expanding url %r" % (self.name, e))
            
            droplet['links'].append({'url': link})
            
        # Send back the updated droplet to the droplet queue for updating
        droplet['links_complete'] = True
        self.drop_publisher.publish(droplet)
                
        # Confirm delivery only once droplet has been passed
        # for metadata extraction
        self.confirm_queue.put(delivery_tag, False)
        log.info(" %s finished processing" % (self.name,))
        
class LinkExtractorQueueDaemon(Daemon):
    def __init__(self, num_workers, mq_host, pid_file, out_file):
        Daemon.__init__(self, pid_file, out_file, out_file, out_file)
        
        self.num_workers = num_workers
        self.mq_host = mq_host
    
    def run(self):
        options = {'exchange_name': 'metadata', 
                   'exchange_type': 'fanout', 
                   'durable_queue': True,
                   'prefetch_count': self.num_workers}
        drop_consumer = Consumer("linkextractor-consumer", self.mq_host, 
                                 'LINK_EXTRACTOR_QUEUE', options)        
        drop_publisher = DropPublisher(mq_host)
                
        for x in range(self.num_workers):
            LinkExtractorQueueWorker("linkextractor-worker-" + str(x), 
                                     drop_consumer.message_queue, 
                                     drop_consumer.confirm_queue, 
                                     drop_publisher)
        log.info("Workers started")
        
        drop_consumer.join()
        log.info("Exiting")
            
if __name__ == "__main__":
    config = ConfigParser.SafeConfigParser()
    config.readfp(open(dirname(realpath(__file__))+'/config/linkextractor.cfg'))
    
    try:
        log_file = config.get("main", 'log_file')
        out_file = config.get("main", 'out_file')
        pid_file = config.get("main", 'pid_file')
        num_workers = config.getint("main", 'num_workers')
        log_level = config.get("main", 'log_level')
        mq_host = config.get("main", 'mq_host')
        
        FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        log.basicConfig(filename=log_file, level=getattr(log, log_level.upper()), format=FORMAT)        
        file(out_file, 'a') # Create outfile if it does not exist
        
        daemon = LinkExtractorQueueDaemon(num_workers, mq_host, pid_file, out_file)
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
