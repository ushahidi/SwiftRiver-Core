#!/usr/bin/env python
# encoding: utf-8
"""
Extracts semantics from the droplets posted to the metadata fanout exchange
and publishes the updated droplets back to the DROPLET_QUEUE for updating
in the db

Copyright (c) 2012 Ushahidi. All rights reserved.
"""

import sys
import time
import ConfigParser
import socket
import logging as log
import json
import random
import re
import time
from urllib import urlencode
from threading import Thread, Lock
from os.path import dirname, realpath

from httplib2 import Http

from swiftriver import Worker, Consumer, Daemon, Publisher


class SemanticsQueueWorker(Worker):

    def __init__(self, name, job_queue, confirm_queue, api_url,
                 drop_publisher, lock, max_retries, sleep_time,
                 retry_cache):
        self.api_url = api_url
        self.h = Http()
        self.drop_publisher = drop_publisher
        self.lock = lock
        self.max_retries = max_retries
        self.sleep_time = sleep_time
        self.retry_cache = retry_cache

        Worker.__init__(self, name, job_queue, confirm_queue)

    def work(self):
        """POSTs the droplet to the semantics API"""
        method, properties, body = self.job_queue.get(True)
        delivery_tag = method.delivery_tag
        start_time = time.time()
        droplet = json.loads(body)

        log.info(" %s droplet received with correlation_id %s" %
                 (self.name, properties.correlation_id))

        droplet_raw = droplet['droplet_raw']

        if droplet_raw is not None:
            # nltk.clean_html() implementation
            # Credits: NLTK Team - <http://www.nltk.org>

            # Remove inline Javascript and CSS
            droplet_raw = re.sub(
                r"(?is)<(script|style).*?>.*?(</\1)",
                "",
                droplet_raw.strip())

            # Remove HTML comments. Do this before removing HTML tags because
            # comments can contain '>' characters
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

            post_data = dict(text=droplet_raw)
            headers = {'Content-type': 'application/x-www-form-urlencoded'}

            resp = content = None
            
            # Unique ID used to store the drop in the retry cache
            cache_id = properties.correlation_id

            # Flag to determine whether or not to retry
            # submitting the drop for semantic extraction
            retry_submit = False

            while not resp:
                try:
                    resp, content = self.h.request(self.api_url, 'POST',
                                                   body=urlencode(post_data),
                                                   headers=headers)

                    # If no OK response, keep retrying
                    if resp.status != 200:
                        resp = content = None

                        # Acquire shared lock
                        self.lock.acquire()
                        # Check if the drop is in the retry cache
                        if not self.retry_cache.has_key(cache_id):
                            self.retry_cache[cache_id] = 0

                        # Increment the retry counter
                        self.retry_cache[cache_id] += 1

                        if self.retry_cache[cache_id] <= self.max_retries:
                            log.error(
                                "%s NOK response from the API (%d), retrying" %
                                (self.name, resp.status))
                            retry_submit = True
                        else:
                            # Drop has exceeded maximum number of retries
                            # so purge from retry cache
                            del self.retry_cache[cache_id]
                            retry_submit = False
                        
                        # Release the lock
                        self.lock.release()
                        
                        # If maximum retries have been exhausted, break
                        if not retry_submit:
                            break
                except socket.error, msg:
                    log.error(
                        "%s Error communicating with api(%s). Retrying" %
                        (self.name, msg))
                    time.sleep(self.sleep_time)

            log.info('%s sematics API said %r' % (self.name, content))
            if content:
                response = json.loads(content)

                if response.get('status') == 'OK':
                    semantics = response['results']

                    if 'location' in semantics:
                        droplet['places'] = []
                        for place in semantics['location']:
                            droplet['places'].append({
                                'place_name': place['place_name'],
                                'latitude': place['coordinates']['latitude'],
                                'longitude': place['coordinates']['longitude'],
                                'source': 'gisgraphy'})

                        # Remove gpe items and return the rest as tags
                        del(semantics['location'])

                    droplet['tags'] = []
                    for k, v in semantics.iteritems():
                        for tag in semantics[k]:
                            droplet['tags'].append({
                                'tag_name': tag,
                                'tag_type': k})

                    log.debug('%s droplet meta = %r, %r' %
                              (self.name, droplet.get('tags'),
                               droplet.get('places')))

        # Send back the updated droplet to the droplet queue for updating
        droplet['semantics_complete'] = True
        droplet['source'] = 'semantics'

        # Some internal data for our callback
        droplet['_internal'] = {'delivery_tag': delivery_tag}

        # Publish the drop to it's reply_to queue
        self.drop_publisher.publish(droplet, 
                                    callback=self.confirm_drop,
                                    corr_id=properties.correlation_id,
                                    routing_key=properties.reply_to)

        log.info("%s finished processing in %fs" 
                 %(self.name, time.time()-start_time))

    def confirm_drop(self, drop):
        # Confirm delivery only once droplet has been passed
        # for metadata extraction
        self.confirm_queue.put(drop['_internal']['delivery_tag'], False)


class SemanticsQueueDaemon(Daemon):

    def __init__(self, num_workers, mq_host, api_url, 
                 pid_file, out_file, sleep_time, max_retries):
        Daemon.__init__(self, pid_file, out_file, out_file, out_file)

        self.num_workers = num_workers
        self.api_url = api_url
        self.mq_host = mq_host
        self.lock = Lock()
        self.max_retries = max_retries
        self.sleep_time = sleep_time
        self.retry_cache = {}

    def run(self):
        # Parameters to be passed on to the queue worker
        queue_name = 'SEMANTICS_QUEUE'
        options = {'exchange_name': 'metadata',
                   'exchange_type': 'fanout',
                   'durable_queue': True,
                   'prefetch_count': self.num_workers}

        drop_consumer = Consumer("semanticsqueue-consumer", self.mq_host,
                                 'SEMANTICS_QUEUE', options)

        drop_publisher = Publisher("Response Publisher", mq_host)

        for x in range(self.num_workers):
            SemanticsQueueWorker("semanticsqueue-worker-" + str(x),
                                 drop_consumer.message_queue,
                                 drop_consumer.confirm_queue,
                                 self.api_url, drop_publisher,
                                 self.lock, self.max_retries,
                                 self.sleep_time, self.retry_cache)

        log.info("Workers started")
        drop_consumer.join()
        log.info("Exiting")


if __name__ == "__main__":
    config = ConfigParser.SafeConfigParser()
    config.readfp(open(
        dirname(realpath(__file__)) + '/config/semanticsqueue.cfg'))

    try:
        log_file = config.get("main", 'log_file')
        out_file = config.get("main", 'out_file')
        pid_file = config.get("main", 'pid_file')
        num_workers = config.getint("main", 'num_workers')
        log_level = config.get("main", 'log_level')
        api_url = config.get("main", 'api_url')
        mq_host = config.get("main", 'mq_host')

        # No. of seconds to sleep if an error is encountered
        sleep_time = config.get("main", "sleep_time")

        # Maximum no. of times to retry sending a request for semantic
        # extraction
        max_retries = config.get("main", "max_retries")

        FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        log.basicConfig(filename=log_file,
                        level=getattr(log, log_level.upper()),
                        format=FORMAT)
        # Create outfile if it does not exist
        file(out_file, 'a')

        # Create the daemon reference
        daemon = SemanticsQueueDaemon(num_workers, mq_host, api_url,
                                      pid_file, out_file, sleep_time,
                                      max_retries)
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
