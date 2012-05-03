#!/usr/bin/env python
# encoding: utf-8
"""
Extracts semantics from the droplets posted to the metadata fanout exchange and publishes
the updated droplets back to the DROPLET_QUEUE for updating in the db

Copyright (c) 2012 Ushahidi. All rights reserved.
"""

import sys, time
import ConfigParser
import socket
import logging as log
import json, re
from urllib import urlencode
from threading import Thread
from os.path import dirname, realpath

from httplib2 import Http

from swiftriver import Worker, Consumer, Daemon, DropPublisher

class SemanticsQueueWorker(Worker):
    
    def __init__(self, name, job_queue, confirm_queue, drop_publisher, api_url):
        self.drop_publisher = drop_publisher
        self.api_url = api_url
        self.h = Http()
        Worker.__init__(self, name, job_queue, confirm_queue)
        
    def work(self):
        """POSTs the droplet to the semantics API"""
        routing_key, delivery_tag, body = self.job_queue.get(True)
        droplet = json.loads(body)
        log.info(" %s droplet received with id %d" % (self.name, droplet.get('id', 0)))
        
        #
        # nltk.clean_html() implementation
        # Credits - NLTK Team - <http://www.nltk.org>
        #
        
        # Remove inline Javascript and CSS
        droplet_raw = re.sub(r"(?is)<(script|style).*?>.*?(</\1)", "", droplet['droplet_raw'].strip())
        
        # Remove HTML comments. Do this before removing HTML tags because comments
        # can contain '>' characters
        droplet_raw = re.sub(r"(?s)<!--(.*?)-->[\n]?", "", droplet_raw)
        
        # Remove the remaining HTML tags
        droplet_raw = re.sub(r"(?s)<.*?>", " ", droplet_raw)
        # Whitespace removal
        droplet_raw = re.sub(r"&nbsp;", " ", droplet_raw)
        droplet_raw = re.sub(r"   ", " ", droplet_raw)
        droplet_raw = re.sub(r"   ", " ", droplet_raw)
        
        # Finally, UTF-8 encode the payload before submitting it to the
        # tagging API
        droplet_raw = droplet_raw.strip().encode('utf-8', 'ignore')
        
        if droplet_raw: # Not empty after stripping tags
            post_data = dict(text=droplet_raw)
            #log.debug(" %s post_data = %r" % (self.name, urlencode(post_data)))
            headers = {'Content-type': 'application/x-www-form-urlencoded'}
            
            resp = content = None
            
            while not resp:
                try:
                    resp, content = self.h.request(self.api_url, 'POST', body=urlencode(post_data), headers=headers)
                    
                    # If no OK response, keep retrying
                    if resp.status != 200:
                        log.error(" %s NOK response from the API (%d), retrying." % (self.name, resp.status))
                        resp = content = None
                        time.sleep(60)
                except socket.error, msg:
                    log.error("%s Error communicating with api(%s). Retrying" % (self.name, msg))
                    time.sleep(60) #Retry after 60 seconds
            
            log.debug('%s sematics API said %r' % (self.name, content))
            if content:
                response = json.loads(content)
                
                if response.get('status') == 'OK':
                    semantics = response['results']
                    
                    if semantics.has_key('location'):
                        droplet['places'] = []
                        for place in semantics['location']:
                            droplet['places'].append({
                                'place_name': place['place_name'],
                                'latitude': place['coordinates']['latitude'],
                                'longitude': place['coordinates']['longitude'],
                                'source': 'gisgraphy'
                            })
                        # Remove gpe items and return the rest as tags
                        del(semantics['location'])
                    
                    droplet['tags'] = []
                    for k, v in semantics.iteritems():
                        for tag in semantics[k]:
                            droplet['tags'].append({
                                'tag_name': tag,
                                'tag_type': k
                            })
                    
                    log.debug('%s droplet meta = %r, %r' % (self.name, droplet.get('tags'), droplet.get('places')))
                
        # Send back the updated droplet to the droplet queue for updating
        droplet['semantics_complete'] = True
        self.drop_publisher.publish(droplet)                             
                
        # Confirm delivery only once droplet has been passed
        # for metadata extraction
        self.confirm_queue.put(delivery_tag, False)
        log.info(" %s finished processing" % (self.name,))
        
class SemanticsQueueDaemon(Daemon):
    def __init__(self, num_workers, mq_host, api_url, pid_file, out_file):
        Daemon.__init__(self, pid_file, out_file, out_file, out_file)
        
        self.num_workers = num_workers
        self.api_url = api_url
        self.mq_host = mq_host
    
    def run(self):
        
        # Parameters to be passed on to the queue worker
        queue_name = 'SEMANTICS_QUEUE'
        options = {'exchange_name': 'metadata', 
                   'exchange_type': 'fanout', 
                   'durable_queue': True,
                   'prefetch_count': self.num_workers}
        
        drop_consumer = Consumer("semanticsqueue-consumer", self.mq_host, 
                                 'SEMANTICS_QUEUE', options)
        drop_publisher = DropPublisher(mq_host)
        for x in range(self.num_workers):
            SemanticsQueueWorker("semanticsqueue-worker-" + str(x), 
                                 drop_consumer.message_queue, 
                                 drop_consumer.confirm_queue, drop_publisher,
                                 self.api_url)
            
        log.info("Workers started");
        drop_consumer.join();
        log.info("Exiting");
            
if __name__ == "__main__":
    config = ConfigParser.SafeConfigParser()
    config.readfp(open(dirname(realpath(__file__))+'/config/semanticsqueue.cfg'))
    
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
        
        daemon = SemanticsQueueDaemon(num_workers, mq_host, api_url, pid_file, out_file)
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
