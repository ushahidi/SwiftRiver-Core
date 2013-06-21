#!/usr/bin/env python
# encoding: utf-8
"""
Schedules RSS fetches by placing URLs in the RSS_FETCHQUEUE which will be
processed by fetch workers which will respond via the RSS_FETCH_RESPONSE
queue in the MQ.

Maintains an in memory and db list of the last fetch times of a URL.

Also listens for new rss channel filters and deleted rss channel filters
published to the web app on the chatter exchange in the MQ and updates
its in memory cache accordingly.

Copyright (c) 2012 Ushahidi. All rights reserved.
"""

import sys, time, json
import logging as log
import ConfigParser
import socket
import random
import re
from hashlib import md5
from threading import Thread, RLock
from os.path import dirname, realpath

import MySQLdb

from swiftriver import Publisher, Consumer, Worker, Daemon


class RssFetchScheduler(Daemon):
    """Queues urls to be processed by rss fetcher workers"""

    FETCH_INTERVAL = 1800

    def __init__(self, num_response_workers, num_channel_update_workers,
                 mq_host, db_config, pid_file, out_file):
        Daemon.__init__(self, pid_file, out_file, out_file, out_file)
        
        self.rss_urls = {}
        self.num_response_workers = num_response_workers
        self.num_channel_update_workers = num_channel_update_workers
        self.mq_host = mq_host
        self.db_config = db_config
        self.db = None
        self.lock = RLock()

    def get_cursor(self):
        """Get a db connection and attempt reconnection"""
        cursor = None
        while not cursor:
            try:
                if not self.db:
                    self.db = MySQLdb.connect(host=self.db_config['host'],
                                                port=self.db_config['port'],
                                                passwd=self.db_config['pass'],
                                                user=self.db_config['user'],
                                                db=self.db_config['database'])

                self.db.ping(True)
                cursor = self.db.cursor()
            except MySQLdb.OperationalError:
                log.error(" error connecting to db, retrying")
                time.sleep(60)

        return cursor;

    def get_river_channel_urls(self):
        """
        Get a list of urls from the river_channels table
        and return with the rivers ids grouped per url
        """

        c = self.get_cursor()
        c.execute("""
        select rc.id AS channel_id, river_id, parameters
        from rivers r, river_channels rc
        where r.id = rc.river_id
        and r.river_active = 1
        and rc.channel = 'rss'
        and rc.active = 1
        """)

        urls = {}
        for channel_id, river_id, parameters in c.fetchall():
            url = json.loads(parameters)['value']
            if not urls.has_key(url):
                urls[url] = {'rivers': set(), 'channels': set()}

            urls[url]['rivers'].add(int(river_id))
            urls[url]['channels'].add(int(channel_id))

        c.close()
        log.debug("river_channel_urls fetched %r" % urls)
        return urls

    def get_rss_urls(self):
        """
        Get a list of previously fetched urls from the rss_urls table
        """
        c = self.get_cursor();
        c.execute("""
        select url, last_fetch_time, last_fetch_etag, last_fetch_modified
        from rss_urls
        """)

        for url, last_fetch_time, last_fetch_etag, last_fetch_modified in c.fetchall():
            self.rss_urls[url] = {
                'last_fetch_time': last_fetch_time or 0,
                'next_fetch_time': self.get_next_fetch_time(last_fetch_time or 0),
                'last_fetch_etag': last_fetch_etag,
                'last_fetch_modified': last_fetch_modified,
                'submitted': False
            }

        c.close()
        log.debug("rss_urls fetched %r" % self.rss_urls)

    def get_next_fetch_time(self, last_fetch_time):
        """Get a next fetch time with some added entropy to spread out
        workloads.
        """
        return last_fetch_time + self.FETCH_INTERVAL + random.randint(0, self.FETCH_INTERVAL)

    def init_cache(self):
        """ Initializes the local url cache """
        self.get_rss_urls()
        cached_urls = set([url for url in self.rss_urls])
        river_channels = self.get_river_channel_urls()
        river_channel_urls = set([url for url in river_channels])

        with self.lock:
            # Add urls missing in the local cache that exist in the
            # channel filters table
            missing_urls =  river_channel_urls - cached_urls
            added_urls = []
            for url in missing_urls:
                row = (url, md5(hash))
                added_urls.append(added_urls)

            log.info("%d urls have been added." % len(added_urls))
            
            if len(added_urls):
                c = self.get_cursor()
                c.executemany(
                    "insert into rss_urls (url, url_hash) values (%s)", added_urls
                )
                c.close()
                self.db.commit()
                for url in added_urls:
                    self.rss_urls[url] = {
                        'last_fetch_time': 0,
                        'next_fetch_time': 0,
                        'last_fetch_etag': None,
                        'last_fetch_modified': None,
                        'submitted': False
                    }

            # Remove rivers from the local cache that have been removed
            # from the channel filters table
            deleted_urls = cached_urls - river_channel_urls
            deleted_url_hashes = set([md5(url) for url in deleted_urls])

            if len(deleted_urls):
                log.info("%d urls have been deleted." % len(deleted_url_hashes))
                log.debug(deleted_urls)

                c = self.get_cursor()
                c.executemany(
                    "delete from rss_urls where url_hash in (%s)",
                    deleted_url_hashes)
                c.close()
                self.db.commit()

                for url in deleted_urls:
                    del(self.rss_urls[url])

            # Add river ids to the in memory cache
            for url in self.rss_urls:
                self.rss_urls[url]['rivers'] = river_channels[url]['rivers']
                self.rss_urls[url]['channels'] = river_channels[url]['channels']
                
        log.info("%d urls loaded" % len(self.rss_urls))

    def run_scheduler(self):
        """
        Submits URLs to the MQ that are past their next_fetch_time
        and marks them as submitted
        """
        log.info("Starting scheduler")
        while True:
            # Get all non submitted urls that are due for a fetch based
            # on the last_fetch time
            jobs = []
            with self.lock:
                jobs = [{"url": url,
                         "next_fetch_time": self.rss_urls[url]["next_fetch_time"],
                         "submitted": self.rss_urls[url]["submitted"]
                         } for url in self.rss_urls]

                jobs = filter(lambda x: (not x["submitted"]
                                         and time.mktime(time.gmtime()) > x["next_fetch_time"]),
                                    jobs)
                jobs.sort(key=lambda x: x["next_fetch_time"])
                log.debug("job_list = %r" % jobs)

                # Submit the url to the fetchers
                for job in jobs:
                    self.rss_urls[job['url']]['submitted'] = True
                    self.fetch_publisher.publish({
                        'url': job['url'],
                        'river_ids': list(self.rss_urls[job['url']]['rivers']),
                        'channel_ids': list(self.rss_urls[job['url']]['channels']),
                        'last_fetch_time': self.rss_urls[job['url']]['last_fetch_time'],
                        'last_fetch_etag': self.rss_urls[job['url']]['last_fetch_etag'],
                        'last_fetch_modified': self.rss_urls[job['url']]['last_fetch_modified'],
                        'use_cache': False
                    })

            time.sleep(60)

    def add_url(self, url, river_id, channel_id):
        with self.lock:
            if not self.rss_urls.has_key(url):
                log.debug(" Adding new url %s for river id %d" % (url, river_id))
                self.rss_urls[url] = {
                    'last_fetch_time': 0,
                    'next_fetch_time': 0,
                    'last_fetch_etag': None,
                    'last_fetch_modified': None,
                    'submitted': False,
                    'rivers': set(),
                    'channels': set()
                }
            self.rss_urls[url]['rivers'].add(river_id);
            self.rss_urls[url]['channels'].add(channel_id)
            self.fetch_publisher.publish({
                'url': url,
                'river_ids': list(self.rss_urls[url]['rivers']),
                'channel_ids': list(self.rss_urls[url]['channels']),
                'last_fetch_time': self.rss_urls[url]['last_fetch_time'],
                'last_fetch_etag': self.rss_urls[url]['last_fetch_etag'],
                'last_fetch_modified': self.rss_urls[url]['last_fetch_modified'],
                'use_cache': True
            })

    def del_url(self, url, river_id, channel_id):
        with self.lock:
            if self.rss_urls.has_key(url):
                if river_id in self.rss_urls[url]['rivers']:
                    log.debug(" Deleting url %s for river id %d" % (url, river_id))
                    # Remove the river id from local url cache
                    self.rss_urls[url]['rivers'].remove(river_id)
                    self.rss_urls[url]['rivers'].remove(channel_id)
                    
                    # If no more rivers associated with a url
                    if not self.rss_urls[url]['rivers']:
                        log.debug(" URL %s has no more rivers. Removing." % (url,))
                        del self.rss_urls[url]
                    
    def update_rss_url(self, update):
        """ Used by the ResponseThread to update the memory and db copies of the rss_url
        with the etag, modified and last_fetch_time
        """
        with self.lock:
            if update['url'] in self.rss_urls:
                self.rss_urls[update['url']]['last_fetch_etag'] = update['last_fetch_etag']
                self.rss_urls[update['url']]['last_fetch_modified'] = update['last_fetch_modified']
                self.rss_urls[update['url']]['last_fetch_time'] = update['last_fetch_time']
                self.rss_urls[update['url']]['next_fetch_time'] = self.get_next_fetch_time(update['last_fetch_time'])
                self.rss_urls[update['url']]['submitted'] = False
                c = self.get_cursor()
                c.execute(
                    """update rss_urls set
                    last_fetch_time = %s,
                    last_fetch_etag = %s,
                    last_fetch_modified = %s
                    where url = %s""",
                    (update['last_fetch_time'], update['last_fetch_etag'], update['last_fetch_modified'], update['url'])
                )
                c.close()
                self.db.commit()


    def run(self):
        log.info("SwiftRiver RSS Fetcher Started")
        
        self.init_cache()
        self.fetch_publisher = RssFetchPublisher(self.mq_host)

        # Start a pool of threads to handle responses from
        # fetchers and update rss_urls
        fetcher_response_consumer = Consumer("fetcher-response-consumer",
                                             self.mq_host,
                                             'RSS_FETCH_RESPONSE',
                                             {'durable_queue': False,
                                              'prefetch_count': self.num_response_workers})

        for x in range(self.num_response_workers):
            FetcherResponseHandler("response-handler-" + str(x),
                                   fetcher_response_consumer.message_queue,
                                   fetcher_response_consumer.confirm_queue,
                                   self)

        # Start a pool to handle new/removed channel options from the web front end / wherever
        # Update the options
        options = {'exchange_name': 'chatter',
                   'exchange_type': 'topic',
                   'routing_key': ['web.river.*',
                                   'web.channel.rss.*'],
                   'durable_exchange':  True,
                   'prefetch_count': self.num_channel_update_workers}
        channel_update_consumer = Consumer("channel-update-consumer",
                                           self.mq_host,
                                           'RSS_UPDATE_QUEUE', options)

        for x in range(self.num_channel_update_workers):
            ChannelUpdateHandler("channel-handler-" + str(x),
                                 channel_update_consumer.message_queue,
                                 channel_update_consumer.confirm_queue, self)

        self.run_scheduler()


class FetcherResponseHandler(Worker):
    """
    Worker thread responsible for fetching articles and putting
    them on the droplet queue
    """

    def __init__(self, name, job_queue, confirm_queue, scheduler):
        self.scheduler = scheduler
        Worker.__init__(self, name, job_queue, confirm_queue)

    def work(self):
        """Update rss_url status from fetcher response"""
        try:
            method, properties, body = self.job_queue.get(True)
            delivery_tag = method.delivery_tag
            message = json.loads(body)
            log.debug(" %s response received %r" % (self.name, message))
            self.scheduler.update_rss_url(message)
            self.confirm_queue.put(delivery_tag, False)
            log.debug(" %s rss_urls update" % (self.name))
        except Exception, e:
            log.info(e);
            #Catch unhandled exceptions
            log.exception(e)


class ChannelUpdateHandler(Worker):
    """Thread responsible for waiting for new/deleted channel options
    from the web front end application"""

    def __init__(self, name, job_queue, confirm_queue, scheduler):
        self.scheduler = scheduler
        Worker.__init__(self, name, job_queue, confirm_queue)

    def work(self):
        """Fetch a newly added channel option"""
        try:
            method, properties, body = self.job_queue.get(True)
            routing_key = method.routing_key
            delivery_tag = method.delivery_tag
            message = json.loads(body)
            
            if (re.search("^web.channel", routing_key, re.I)):
                if message['channel'] == 'rss':
                    log.debug(" %s %s channel received %r" % (self.name, routing_key, message))
                    url = json.loads(message['parameters'])['value']
                    river_id = int(message['river_id'])
                    channel_id = int(message['id'])
                    if routing_key == 'web.channel.rss.add':
                        self.scheduler.add_url(url, river_id, channel_id)
                    elif routing_key == 'web.channel.rss.delete':
                        self.scheduler.del_url(url, river_id, channel_id)
            elif (re.search("^web.river", routing_key, re.I)):
                log.debug(" %s %s received %r" % (self.name, routing_key, message))
                for channel_option in message['channels']:
                    url = json.loads(channel_option['parameters'])['value']
                    river_id = int(channel_option['river_id'])
                    channel_id = int(channel_option['channel_id'])
                    channel = channel_option['channel']

                    if routing_key == 'web.river.enable' and \
                        channel == 'rss':
                        self.scheduler.add_url(url, river_id, channel_id)
                    elif routing_key == 'web.river.disable' and  \
                          channel == 'rss':
                        self.scheduler.del_url(url, river_id, channel_id)

            self.confirm_queue.put(delivery_tag, False)
            log.debug(" %s channel option processed" % (self.name))
        except Exception, e:
            log.info(e);
            #Catch unhandled exceptions
            log.exception(e)

class RssFetchPublisher(Publisher):

    def __init__(self, mq_host):
        Publisher.__init__(self, "RSS Fetch Queue Publisher", mq_host,
                           queue_name='RSS_FETCH_QUEUE', durable=False)

if __name__ == "__main__":
    config = ConfigParser.SafeConfigParser()
    config.readfp(open(dirname(realpath(__file__))+'/config/rss_scheduler.cfg'))

    try:
        log_file = config.get("main", 'log_file')
        out_file = config.get("main", 'out_file')
        pid_file = config.get("main", 'pid_file')
        num_response_workers = config.getint("main", 'num_response_workers')
        num_channel_update_workers = config.getint("main", 'num_channel_update_workers')
        log_level = config.get("main", 'log_level')
        mq_host = config.get("main", 'mq_host')
        db_config = {
            'host': config.get("db", 'host'),
            'port': config.getint("db", 'port'),
            'user': config.get("db", 'user'),
            'pass': config.get("db", 'pass'),
            'database': config.get("db", 'database')
        }

        FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        log.basicConfig(filename=log_file,
                        level=getattr(log, log_level.upper()), format=FORMAT)

        # Create outfile if it does not exist
        file(out_file, 'a')

        daemon = RssFetchScheduler(num_response_workers,
                                   num_channel_update_workers,
                                   mq_host, db_config, pid_file, out_file)
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
