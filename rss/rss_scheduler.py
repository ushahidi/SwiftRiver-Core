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

    def get_channel_filter_urls(self):
        """
        Get a list of urls from the channel_filter_options table
        and return with the rivers ids grouped per url
        """

        c = self.get_cursor()
        c.execute("""
        select river_id, value
        from rivers r, channel_filters cf, channel_filter_options cfo
        where r.id = cf.river_id
        and cf.id = cfo.channel_filter_id
        and r.river_active = 1
        and cf.channel = 'rss'
        and cfo.key = 'url'
        and filter_enabled = 1
        """)

        urls = {}
        for river_id, value in c.fetchall():
            url = json.loads(value)['value']
            if not urls.has_key(url):
                urls[url] = []
            urls[url].append(int(river_id))

        c.close()
        log.debug("channel_filter_urls fetched %r" % urls)
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

        urls = {}
        for url, last_fetch_time, last_fetch_etag, last_fetch_modified in c.fetchall():
            urls[url] = {
                'last_fetch_time': last_fetch_time or 0,
                'next_fetch_time': self.get_next_fetch_time(last_fetch_time or 0),
                'last_fetch_etag': last_fetch_etag,
                'last_fetch_modified': last_fetch_modified,
                'submitted': False
            }

        c.close()
        log.debug("rss_urls fetched %r" % urls)
        return urls

    def get_next_fetch_time(self, last_fetch_time):
        """Get a next fetch time with some added entropy to spread out
        workloads.
        """
        return last_fetch_time + self.FETCH_INTERVAL + random.randint(0, self.FETCH_INTERVAL)

    def add_new_urls(self):
        """
        Adds the urls in channel_filter_options and missing in self.rss_urls
        to the rss_urls table and the self.rss_urls list
        """
        with self.lock:
            added_urls = set([url for url in self.channel_filter_urls]) - set([url for url in self.rss_urls])
            log.info("%d urls have been added." % len(added_urls))
            log.debug(added_urls)
            if len(added_urls):
                c = self.get_cursor()
                c.executemany(
                    "insert into rss_urls (url) values (%s)", added_urls
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
        return self


    def remove_deleted_urls(self):
        """
        Removes the urls in rss_urls missing from channel_filter_options from
        the rss_urls table and the self.rss_urls list
        """
        with self.lock:
            deleted_urls = set([url for url in self.rss_urls]) - set([url for url in self.channel_filter_urls])
            log.info("%d urls have been deleted." % len(deleted_urls))
            log.debug(deleted_urls)
            if len(deleted_urls):
                c = self.get_cursor()
                c.executemany(
                    "delete from rss_urls where url in (%s)", deleted_urls
                )
                c.close()
                self.db.commit()
                for url in deleted_urls:
                    del(self.rss_urls[url])
        return self

    def run_scheduler(self):
        """
        Submits URLs to the MQ that are past their next_fetch_time
        and marks them as submitted
        """
        log.info("Starting scheduler")
        while True:
            # Get all non submitted urls that are due for a fetch based on the last_fetch time
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
                        'river_ids': self.channel_filter_urls[job['url']],
                        'last_fetch_time': self.rss_urls[job['url']]['last_fetch_time'],
                        'last_fetch_etag': self.rss_urls[job['url']]['last_fetch_etag'],
                        'last_fetch_modified': self.rss_urls[job['url']]['last_fetch_modified'],
                        'use_cache': False
                    })

            time.sleep(60)

    def add_url(self, channel_option):
        url = json.loads(channel_option['value'])['value'];

        with self.lock:
            if not self.channel_filter_urls.has_key(url):
                log.info(" Adding new url %s for river id %d" % (url, int(channel_option['river_id'])))
                self.channel_filter_urls[url] = []
                self.rss_urls[url] = {
                    'last_fetch_time': 0,
                    'next_fetch_time': 0,
                    'last_fetch_etag': None,
                    'last_fetch_modified': None,
                    'submitted': False
                }
            self.channel_filter_urls[url].append(int(channel_option['river_id']));
            self.fetch_publisher.publish({
                'url': url,
                'river_ids': self.channel_filter_urls[url],
                'last_fetch_time': self.rss_urls[url]['last_fetch_time'],
                'last_fetch_etag': self.rss_urls[url]['last_fetch_etag'],
                'last_fetch_modified': self.rss_urls[url]['last_fetch_modified'],
                'use_cache': True
            })

    def del_url(self, channel_option):
        url = json.loads(channel_option['value'])['value'];
        river_id = int(channel_option['river_id'])

        with self.lock:
            if self.channel_filter_urls.has_key(url):
                if river_id in self.channel_filter_urls[url]:
                    self.channel_filter_urls[url].remove(river_id)



    def update_rss_url(self, update):
        """ Used by the ResponseThread to update the memory and db copies of the rss_url
        with the etag, modified and last_fetch_time
        """
        with self.lock:
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
        self.channel_filter_urls = self.get_channel_filter_urls()
        self.rss_urls = self.get_rss_urls()

        # Refresh our url cache from the channel_filter_options table
        self.add_new_urls().remove_deleted_urls()
        log.info("%d urls loaded" % len(self.rss_urls))

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
                   'routing_key': 'web.channel_option.rss.*',
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
            log.debug(" %s channel option received %r" % (self.name, message))
            # Submit the channel option to the fetchers
            if (message['key'] == 'url' and message['channel'] == 'rss'
                and routing_key == 'web.channel_option.rss.add'):
                self.scheduler.add_url(message);

            if (message['key'] == 'url' and message['channel'] == 'rss'
                and routing_key == 'web.channel_option.rss.delete'):
                self.scheduler.del_url(message);

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
