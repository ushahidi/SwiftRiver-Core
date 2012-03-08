#!/usr/bin/env python
# encoding: utf-8
"""
Schedules RSS fetches by placing URLs in the RSS_FETCHQUEUE which will be processed
by fetch workers which will respond via the RSS_FETCH_RESPONSE queue in the MQ.

Maintains an in memory and db list of the last fetch times of a URL.

Also listens for new rss channel filters and deleted rss channel filters published 
to the web app on the chatter exchange in the MQ and updates its in memory cache
accordingly.

Copyright (c) 2012 Ushahidi. All rights reserved.
"""

import sys, time, json
import logging as log
import MySQLdb
import pika
import ConfigParser
import socket
from threading import Thread, RLock
from os.path import dirname, realpath
from swiftriver import Worker, Daemon


class RssFetchScheduler:    
    """Queues urls to be processed by rss fetcher workers"""

    MAX_FETCH_INTERVAL = 900
    FETCHER_QUEUE = 'RSS_FETCH_QUEUE'    
    
    def __init__(self, num_response_workers, num_channel_update_workers, mq_host, db_config):
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
        from channel_filters cf, channel_filter_options cfo
        where cf.id = cfo.channel_filter_id
        and cf.channel = 'rss'
        and cfo.key = 'url'
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
                'last_fetch_etag': last_fetch_etag,
                'last_fetch_modified': last_fetch_modified,
                'submitted': False
            }
                        
        c.close()
        log.debug("rss_urls fetched %r" % urls)
        return urls
        
    def add_new_urls(self):
        """
        Adds the urls in channel_filter_options and missing in rss_urls to the rss_urls table
        and the self.rss_urls list
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
                        'last_fetch_etag': None,
                        'last_fetch_modified': None,
                        'submitted': False
                    }
        return self
        
        
    def remove_deleted_urls(self):
        """
        Removes the urls in rss_urls missing from channel_filter_options from the rss_urls table
        and the self.rss_urls list
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
        Submits URLs to the MQ that were last fetched more than MAX_FETCH_INTERVAL
        and marks them as submitted
        """
        while True:
            try:
                log.info("Starting scheduler")
                connection = pika.BlockingConnection(pika.ConnectionParameters(
                        host=self.mq_host))
                while True:
                    # Prepare the job queue
                    channel = connection.channel()
                    channel.queue_declare(queue=self.FETCHER_QUEUE, durable=False)
                    
                    # Get all non submitted urls that are due for a fetch based on the last_fetch time
                    jobs = []
                    with self.lock:
                        jobs = [{"url": url, 
                                 "last_fetch_time": self.rss_urls[url]["last_fetch_time"],
                                 "submitted": self.rss_urls[url]["submitted"]
                                 } for url in self.rss_urls]
                    
                    jobs = filter(lambda x: (not x["submitted"] 
                                             and time.mktime(time.gmtime()) - x["last_fetch_time"] > self.MAX_FETCH_INTERVAL), 
                                        jobs)
                    jobs.sort(key=lambda x: x["last_fetch_time"])
                
                    log.debug("job_list = %r" % jobs)
                    
                    # Submit the url to the fetchers
                    for job in jobs:
                        with self.lock:
                            self.rss_urls[job['url']]['submitted'] = True
                            message = json.dumps({
                                'url': job['url'],
                                'river_ids': self.channel_filter_urls[job['url']],
                                'last_fetch_time': self.rss_urls[job['url']]['last_fetch_time'],
                                'last_fetch_etag': self.rss_urls[job['url']]['last_fetch_etag'],
                                'last_fetch_modified': self.rss_urls[job['url']]['last_fetch_modified'],
                                'use_cache': False
                            })
                            channel.basic_publish(exchange='',
                                                  routing_key=self.FETCHER_QUEUE,
                                                  body=message)
                        
                    time.sleep(5)
            except socket.error, msg:
                log.error(" scheduler error connecting to the MQ, retrying")
                time.sleep(60)
            except pika.exceptions.AMQPConnectionError, e:
                log.error(" scheduler lost connection to the MQ, reconnecting")
                time.sleep(60)
            except pika.exceptions.ChannelClosed, e:
                log.error(" scheduler lost connection to the MQ, reconnecting")
                time.sleep(60)
                
    def add_url(self, name, mq, channel_option):
        url = json.loads(channel_option['value'])['value'];
        
        with self.lock:
            if not self.channel_filter_urls.has_key(url):
                log.info(" %s adding new url %s for river id %d" % (name, url, int(channel_option['river_id'])))
                self.channel_filter_urls[url] = []
                self.rss_urls[url] = {
                    'last_fetch_time': 0,
                    'last_fetch_etag': None,
                    'last_fetch_modified': None,
                    'submitted': False
                }        
            self.channel_filter_urls[url].append(int(channel_option['river_id']));    
            
            channel = mq.channel()
            channel.queue_declare(queue=self.FETCHER_QUEUE, durable=False)
            message = json.dumps({
                'url': url,
                'river_ids': self.channel_filter_urls[url],
                'last_fetch_time': self.rss_urls[url]['last_fetch_time'],
                'last_fetch_etag': self.rss_urls[url]['last_fetch_etag'],
                'last_fetch_modified': self.rss_urls[url]['last_fetch_modified'],
                'use_cache': True
            })
            channel.basic_publish(exchange='',
                                  routing_key=self.FETCHER_QUEUE,
                                  body=message)
      
    def del_url(self, name, mq, channel_option):
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
        
            
    def start(self):
        """Begin processing in two separate threads of execution"""
        log.info("SwiftRiver RSS Fetcher Started")
        self.channel_filter_urls = self.get_channel_filter_urls()
        self.rss_urls = self.get_rss_urls()
        
        # Refresh our url cache from the channel_filter_options table
        self.add_new_urls().remove_deleted_urls()
        log.info("%d urls loaded" % len(self.rss_urls))
        
        # Start a pool of threads to handle responses from 
        # fetchers and update rss_urls 
        fetch_queue = 'RSS_FETCH_RESPONSE'
        options = {'scheduler': self, 'durable_queue': False}
        
        for x in range(self.num_response_workers):
            FetcherResponseHandler("response-handler-" + str(x), self.mq_host, 
                                   fetch_queue, options).start()
        
        # Start a pool to handle new/removed channel options from the web front end / wherever
        update_queue = 'RSS_UPDATE_QUEUE'
        
        # Update the options
        options['exchange_name'] = 'chatter'
        options['exchange_type'] = 'topic'
        options['routing_key'] = 'web.channel_option.rss.*';
        options['durable_exchange'] = True
        
        for x in range(self.num_channel_update_workers):
            ChannelUpdateHandler("channel-handler-" + str(x), self.mq_host, 
                                 update_queue, options).start()        
        
        Thread(target=self.run_scheduler).start()

        
class FetcherResponseHandler(Worker):
    """
    Worker thread responsible for fetching articles and putting 
    them on the droplet queue
    """
    
    def __init__(self, name, mq_host, queue, options=None):
        Worker.__init__(self, name, mq_host, queue, options)
        self.scheduler = options.get('scheduler')
    
    def handle_mq_response(self, ch, method, properties, body):
        """Update rss_url status from fetcher response"""
        try:
            message = json.loads(body)
            log.debug(" %s response received %r" % (self.name, message))
            self.scheduler.update_rss_url(message)
            ch.basic_ack(delivery_tag = method.delivery_tag)
            log.debug(" %s rss_urls update" % (self.name))
        except Exception, e:
            log.info(e);
            #Catch unhandled exceptions
            log.exception(e)

 
class ChannelUpdateHandler(Worker):
    """Thread responsible for waiting for new/deleted channel options 
    from the web front end application"""
    
    def __init__(self, name, mq_host, queue, options=None):
        Worker.__init__(self, name, mq_host, queue, options)
        self.scheduler = self.options.get('scheduler')

    def handle_mq_response(self, ch, method, properties, body):
        """Fetch a newly added channel option"""
        try:
            message = json.loads(body)
            log.debug(" %s channel option received %r" % (self.name, message))
            # Submit the channel option to the fetchers
            if (message['key'] == 'url' and message['channel'] == 'rss' 
                and method.routing_key == 'web.channel_option.rss.add'):
                self.scheduler.add_url(self.name, self.mq, message);
            
            if (message['key'] == 'url' and message['channel'] == 'rss' 
                and method.routing_key == 'web.channel_option.rss.add'):
                self.scheduler.del_url(self.name, self.mq, message);
            
            ch.basic_ack(delivery_tag = method.delivery_tag)
            log.debug(" %s channel option processed" % (self.name))
        except Exception, e:
            log.info(e);
            #Catch unhandled exceptions
            log.exception(e)      

            
class RssFetchSchedulerDaemon(Daemon):
    def __init__(self, num_response_workers, num_channel_update_workers, mq_host, db_config, pid_file, out_file):
        Daemon.__init__(self, pid_file, out_file, out_file, out_file)
        
        self.num_response_workers = num_response_workers
        self.mq_host = mq_host
        self.db_config = db_config
    
    def run(self):
        try:
            RssFetchScheduler(self.num_response_workers, 
                              num_channel_update_workers, 
                              self.mq_host, self.db_config).start()
        finally:
            log.info("Exiting");

            
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
        
        daemon = RssFetchSchedulerDaemon(num_response_workers, 
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
