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
import pika
import socket
import json
import ConfigParser
from httplib2 import Http
from httplib import BadStatusLine
from os.path import realpath, dirname
from swiftriver import Worker, Daemon


class DropletQueueWorker(Worker):
    
    def __init__(self, name, mq_host, queue, options=None):
        Worker.__init__(self, name, mq_host, queue, options)
        self.start()
    
    
    def handle_mq_response(self, ch, method, properties, body):
        """POSTs the droplet to the droplet API"""
        
        droplet = json.loads(body)
        log.info(" %s droplet received with id %d channel '%s'" 
                 % (self.name, droplet.get('id', 0), droplet.get('channel', '')))
        
        resp = content = None
        h = Http()
        api_url = self.options.get('api_url')
        
        while not resp:
            try:
                resp, content = h.request(api_url, 'POST', body=json.dumps(droplet))
                
                # If no OK response, keep retrying
                if resp.status != 200:
                    log.error(" %s NOK response from the API (%d), retrying." 
                              % (self.name, resp.status))
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
        
        self.__num_workers = num_workers
        self.__api_url = api_url
        self.__mq_host = mq_host
    
    def run(self):
        # Options to be passed on to the worker thread
        options = {"api_url": self.__api_url, 'durable_queue': True}
        
        for x in range(self.__num_workers):
            DropletQueueWorker("dropletqueue-worker-" + str(x), self.__mq_host, 
                               Worker.DROPLET_QUEUE, options)
        
        log.info("Workers started");
        while True:
            time.sleep(60)
            
if __name__ == "__main__":
    config = ConfigParser.SafeConfigParser()
    config.readfp(open(dirname(realpath(__file__))+'/config/dropletqueue.cfg'))
    
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
