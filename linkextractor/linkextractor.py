#!/usr/bin/env python
# encoding: utf-8
"""
Extracts links from the droplets posted to the metadata fanout exchange and publishes
the update droplet back to the DROPLET_QUEUE for updating in the db

Copyright (c) 2012 Ushahidi. All rights reserved.
"""

import sys, time
import ConfigParser
import socket
import logging as log
import MySQLdb
import pika
import json, re
from urllib import urlencode
from daemon import Daemon
from threading import Thread, Event
from httplib2 import Http, SSLHandshakeError, ServerNotFoundError
from os.path import dirname, realpath

class LinkExtractorQueue(Thread):
    
    LINK_EXTRACTOR_QUEUE = 'LINK_EXTRACTOR_QUEUE'
    DROPLET_QUEUE = 'DROPLET_QUEUE'
    
    def __init__(self, name, mq_host, event):
        Thread.__init__(self)
        self.daemon = True
        self.name = name
        self.event = event
        self.mq_host = mq_host
        self.start()
    
    def run(self):
        """Register our handler for fetcher responses"""
        log.info("Registering droplet handler %s" % self.name)
        
        # Connect to the MQ, retry on failure
        while True:
            try:
                self.mq = pika.BlockingConnection(pika.ConnectionParameters(
                        host=self.mq_host))
                semantics_channel = self.mq.channel()
                semantics_channel.exchange_declare(exchange='metadata', type='fanout', durable=True)        
                semantics_channel.queue_declare(queue=self.LINK_EXTRACTOR_QUEUE, durable=True)
                semantics_channel.queue_bind(exchange='metadata',queue=self.LINK_EXTRACTOR_QUEUE)
                semantics_channel.basic_qos(prefetch_count=1)
                semantics_channel.basic_consume(self.handle_droplet,
                                      queue=self.LINK_EXTRACTOR_QUEUE)            
                semantics_channel.start_consuming()
            except socket.error, msg:
                log.error("%s error connecting to the MQ, retrying" % (self.name))
                time.sleep(60)
            except pika.exceptions.AMQPConnectionError, e:
                log.error("%s lost connection to the MQ, reconnecting" % (self.name))
                time.sleep(60)
    
    def handle_droplet(self, ch, method, properties, body):
        """POSTs the droplet to the semantics API"""
        droplet = None
        try:
            droplet = json.loads(body)
        except ValueError, e:
            # Bad value in the queue, skip it
            log.error(" %s bad value received in the queue" % (self.name,))
            ch.basic_ack(delivery_tag = method.delivery_tag)
            return
            
        log.info(" %s droplet received with id %d" % (self.name, droplet.get('id', 0)))
        
        # Credit to https://gist.github.com/729294
        pattern = "(?:(?:https?|ftp)://)"
        pattern += "(?:\\S+(?::\\S*)?@)?"
        pattern += "(?:"
        pattern += "(?!10(?:\\.\\d{1,3}){3})"
        pattern += "(?!127(?:\\.\\d{1,3}){3})"
        pattern += "(?!169\\.254(?:\\.\\d{1,3}){2})"
        pattern += "(?!192\\.168(?:\\.\\d{1,3}){2})"
        pattern += "(?!172\\.(?:1[6-9]|2\\d|3[0-1])(?:\\.\\d{1,3}){2})"
        pattern += "(?:[1-9]\\d?|1\\d\\d|2[01]\\d|22[0-3])"
        pattern += "(?:\\.(?:1?\\d{1,2}|2[0-4]\\d|25[0-5])){2}"
        pattern += "(?:\\.(?:[1-9]\\d?|1\\d\\d|2[0-4]\\d|25[0-4]))"
        pattern += "|"
        pattern += "(?:(?:[a-z\\u00a1-\\uffff0-9]+-?)*[a-z\\u00a1-\\uffff0-9]+)"
        pattern += "(?:\\.(?:[a-z\\u00a1-\\uffff0-9]+-?)*[a-z\\u00a1-\\uffff0-9]+)*"
        pattern += "(?:\\.(?:[a-z\\u00a1-\\uffff]{2,}))"
        pattern += ")"
        pattern += "(?::\\d{2,5})?"
        pattern += "(?:/[^\\s]*)?"
        
        # Strip tags leaving only hyperlinks
        droplet_raw = re.sub(r'<(?!\s*[aA]\s*)[^>]*?>', '', droplet['droplet_raw']).strip().encode('ascii', 'ignore')
        
        for link in re.findall(pattern, droplet_raw):
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
            
            droplet['links'].append(link)
            
        # Send back the updated droplet to the droplet queue for updating
        droplet['links_complete'] = True
        droplet_channel = self.mq.channel()
        droplet_channel.queue_declare(queue=self.DROPLET_QUEUE, durable=True)
        droplet_channel.basic_publish(exchange='',
                              routing_key=self.DROPLET_QUEUE,
                              properties=pika.BasicProperties(
                                    delivery_mode = 2, # make message persistent
                              ),
                              body=json.dumps(droplet))
                
        # Confirm delivery only once droplet has been passed
        # for metadata extraction
        ch.basic_ack(delivery_tag = method.delivery_tag)
        log.info(" %s finished processing" % (self.name,))
        
class LinkExtractorQueueDaemon(Daemon):
    def __init__(self, num_workers, mq_host, pid_file, out_file):
        Daemon.__init__(self, pid_file, out_file, out_file, out_file)
        
        self.num_workers = num_workers
        self.mq_host = mq_host
    
    def run(self):
        event = Event()
        for x in range(self.num_workers): LinkExtractorQueue("linkextractor-worker-" + str(x), self.mq_host, event)
        log.info("Workers started");
        event.wait()
        log.info("Exiting");
            
if __name__ == "__main__":
    config = ConfigParser.SafeConfigParser()
    config.readfp(open(dirname(realpath(__file__))+'/linkextractor.cfg'))
    
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
