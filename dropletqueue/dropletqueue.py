#!/usr/bin/env python
# encoding: utf-8
"""
Creates droplets in the SwiftRiver db by posting droplets published to the DROPLET_QUEUE queue on the 
MQ to the api endpoing /api/droplet and publishes newly created droplets back to the metadata exchange
in the MQ for metadata extraction.

Copyright (c) 2012 Ushahidi. All rights reserved.
"""

import sys, time
import logging as log
import MySQLdb
import pika
import socket
import json
import ConfigParser
from daemon import Daemon
from threading import Thread, Event
from httplib2 import Http
from httplib import BadStatusLine
from os.path import realpath, dirname

class DropletQueue(Thread):
    
    DROPLET_QUEUE = 'DROPLET_QUEUE'
    
    def __init__(self, name, mq_host, api_url, event):
        Thread.__init__(self)
        self.daemon = True
        self.name = name
        self.event = event
        self.api_url = api_url
        self.mq_host = mq_host        
        self.start()
    
    def run(self):
        """Register our handler for fetcher responses"""
        log.info("Registering droplet handler %s" % self.name)
        
        # Start a persisitent connection to the api end point
        self.h = Http()
        
        # Connect to the MQ, retry on failure
        while True:
            try:
                self.mq = pika.BlockingConnection(pika.ConnectionParameters(
                        host=self.mq_host), pika.reconnection_strategies.SimpleReconnectionStrategy())
                droplet_channel = self.mq.channel()
                droplet_channel.queue_declare(queue=self.DROPLET_QUEUE, durable=True)
                droplet_channel.basic_qos(prefetch_count=1)
                droplet_channel.basic_consume(self.handle_droplet,
                                      queue=self.DROPLET_QUEUE)            
                droplet_channel.start_consuming()
            except socket.error, msg:
                log.error("%s error connecting to the MQ (%s), retrying" % (self.name, self.mq_host))
                time.sleep(60)
            except pika.exceptions.AMQPConnectionError, e:
                log.error("%s lost connection to the MQ (%s), reconnecting" % (self.name, self.mq_host))
                time.sleep(60)
    
    def handle_droplet(self, ch, method, properties, body):
        """POSTs the droplet to the droplet API"""
        droplet = json.loads(body)
        log.info(" %s droplet received with id %d channel '%s'" % (self.name, droplet.get('id', 0), droplet.get('channel', '')))
        resp = content = None
        while not resp:
            try:
                resp, content = self.h.request(self.api_url, 'POST', body=json.dumps(droplet))
                
                # If no OK response, keep retrying
                if resp.status != 200:
                    log.error(" %s NOK response from the API (%d), retrying." % (self.name, resp.status))
                    resp = content = None
                    time.sleep(60)
            except socket.error, msg:
                log.error("%s Error communicating with api(%s). Retrying" % (self.name, msg))
                time.sleep(60) #Retry after 60 seconds
            except BadStatusLine, e:
                log.error("%s BadStatusLine Error communicating with api(%s). Retrying" % (self.name, e))
                time.sleep(60) #Retry after 60 seconds
            
        if content:
            # Publish the droplet for metadata extraction
            meta_channel = self.mq.channel()
            meta_channel.exchange_declare(exchange='metadata', type='fanout', durable=True)
            meta_channel.basic_publish(exchange='metadata',
                                  routing_key='',
                                  properties=pika.BasicProperties(
                                        delivery_mode = 2, # make message persistent
                                  ),
                                  body=content)
                                  
        # Confirm delivery only once droplet has been passed
        # for metadata extraction
        ch.basic_ack(delivery_tag = method.delivery_tag)
        
        
class DropletQueueDaemon(Daemon):
    def __init__(self, num_workers, mq_host, api_url, pid_file, out_file):
        Daemon.__init__(self, pid_file, out_file, out_file, out_file)
        
        self.num_workers = num_workers
        self.api_url = api_url
        self.mq_host = mq_host
    
    def run(self):
        event = Event()
        for x in range(self.num_workers): DropletQueue("dropletqueue-worker-" + str(x), self.mq_host, self.api_url, event)
        log.info("Workers started");
        while True:
            time.sleep(60)
            
if __name__ == "__main__":
    config = ConfigParser.SafeConfigParser()
    config.readfp(open(dirname(realpath(__file__))+'/dropletqueue.cfg'))
    
    try:
        log_file = config.get("main", 'log_file')
        out_file = config.get("main", 'out_file')
        pid_file = config.get("main", 'pid_file')
        num_workers = config.getint("main", 'num_workers')
        log_level = config.get("main", 'log_level')
        api_url = config.get("main", 'api_url')
        mq_host = config.get("main", 'mq_host')
        
        FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        log.basicConfig(filename=log_file, level=getattr(log, log_level.upper()), format=FORMAT)        
        file(out_file, 'a') # Create outfile if it does not exist
        
        daemon = DropletQueueDaemon(num_workers, mq_host, api_url, pid_file, out_file)
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
