#!/usr/bin/env python
# encoding: utf-8
"""
Waits for RSS fetch requests from the scheduler via the RSS_FETCH_RESPONSE
queue in the MQ.

Places new droplets on the DROPLETQUEUE in the MQ and maintains the last
50 droplets per url in a db cache table rss_cache.

If the scheduler requests a url from the cache, the fetcher will return
droplets form the cache if available of do a HTTP request if there are not
droplets for the url in the cache.

Copyright (c) 2012 Ushahidi. All rights reserved.
"""

import ConfigParser
import hashlib
import json
import logging as log
import pickle
import re
import sys
import time
from threading import Event
from os.path import dirname, realpath

import feedparser
import MySQLdb

from swiftriver import Daemon, Consumer, Worker, Publisher, DropPublisher


class RssFetcherWorker(Worker):

    def __init__(self, name, job_queue, confirm_queue, db_config,
                 drop_publisher, response_publisher):
        self.db_config = db_config
        self.drop_publisher = drop_publisher
        self.response_publisher = response_publisher
        self.db = None
        Worker.__init__(self, name, job_queue, confirm_queue)

    def work(self):
        """Process a URL"""
        method, properties, body = self.job_queue.get(True)
        delivery_tag = method.delivery_tag
        start_time = time.time()
        drop_count = 0
        message = json.loads(body)
        log.info(" %s fetching %s" % (self.name, message['url']))

        cache_found = False
        if message.get('use_cache', False):
            c = self.get_cursor()
            c.execute("""
            select `drop`
            from `rss_cache`
            where url = %s
            """, (message['url'],))

            results = c.fetchall()

            if len(results):
                cache_found = True
                log.info(" %s Served %s from the cache" %
                    (self.name, message['url']))

            for db_drop, in results:
                drop_count += 1
                drop = pickle.loads(db_drop)
                drop['river_id'] = message['river_ids']
                self.drop_publisher.publish(drop)

            c.close()

        drops = []
        if not (message.get('use_cache', False) and cache_found):
            # Fetch the feed and use etag/modified headers if they
            # were provided in the last fetch
            d = None

            # Replace instances of "&amp;" with "&" - some RSS endpoints
            # such as Google News RSS return a 302 when the "&" is not
            # present
            fetch_url = re.sub("&amp;", "&", message['url'])
            
            if message['last_fetch_etag']:
                d = feedparser.parse(fetch_url,
                                     etag=message['last_fetch_etag'])
            elif message['last_fetch_modified']:
                d = feedparser.parse(fetch_url,
                                     modified=message['last_fetch_modified'])
            else:
                d = feedparser.parse(fetch_url)

            # Get the avatar, locale and identity name
            avatar = (d.feed.image.get('href', None)
                      if 'image' in d.feed else None)

            locale = d.feed.get('language', None)
            identity_name = d.feed.get('title', message['url'])

            for entry in d.entries:
                drop_count += 1
                if (not message['last_fetch_etag'] and
                    not message['last_fetch_modified']):
                    if (time.mktime(entry.get('date_parsed', time.gmtime()))
                        < int(message['last_fetch_time'])):
                        continue

                content = None
                if 'content' in entry:
                    content = entry.content[0].value
                elif 'summary' in entry:
                    # Publisher probably misbehaving and put content
                    # in the summary
                    content = entry.summary

                # Build out the drop
                droplet_orig_id_str = (entry.get('link', '') +
                                       entry.get('id', ''))
                # Date when the article was published
                droplet_date_pub = time.strftime('%a, %d %b %Y %H:%M:%S +0000',
                                                 entry.get('date_parsed',
                                                           time.gmtime()))
                drop = {
                    'channel': 'rss',
                    'river_id': message['river_ids'],
                    'identity_orig_id': message['url'],
                    'identity_username': d.feed.get('link', message['url']),
                    'identity_name': identity_name,
                    'identity_avatar': avatar,
                    'droplet_orig_id': hashlib.md5(droplet_orig_id_str
                                                   .encode('utf-8'))
                                                   .hexdigest(),
                    'droplet_type': 'original',
                    'droplet_title': entry.get('title', None),
                    'droplet_content': content,
                    'droplet_raw': content,
                    'droplet_locale': locale,
                    'droplet_date_pub': droplet_date_pub}
                    
                if 'link' in entry:
                    drop['links'] = [{
                        'url': entry.get('link'),
                        'original_url': True
                    }]

                #log.debug("Drop: %r" % drop)
                drops.append((message['url'],
                              time.mktime(entry.get('date_parsed',
                                                    time.gmtime())),
                              pickle.dumps(drop)))

                self.drop_publisher.publish(drop)

            # Send the status, etag & modified back the the scheduler
            self.response_publisher.publish({
                'url': message['url'],
                'status': d.get('status', None),
                'last_fetch_modified': d.get('modified',
                                             message['last_fetch_modified']),
                'last_fetch_etag': d.get('etag', message['last_fetch_etag']),
                'last_fetch_time': int(time.mktime(time.gmtime()))})

        self.confirm_queue.put(delivery_tag, False)
        log.info(" %s done fetching %d drops from %s in %fs" % (self.name, 
                                                  drop_count,
                                                  message['url'],
                                                  time.time()-start_time))

        # Add new drops to our local cache
        if len(drops):
            c = self.get_cursor()

            # Some cache maintenance. Only keep 50 recent entries
            c.execute(
                """
                delete from rss_cache
                where url = %s
                and id not in (
                select id
                from (
                select id
                from rss_cache
                where url = %s
                order by date_pub desc
                limit 50
                ) a
                );
                """, (message['url'], message['url'],))

            c.executemany(
                """
                insert into `rss_cache` (`url`, `date_pub`, `drop`)
                values (%s, %s, %s)
                """, drops[:50])

            c.close()
            self.db.commit()

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
                log.error(" error connecting to the database, retrying")
                time.sleep(60 + random.randint(0, 120))

        return cursor


class ResponsePublisher(Publisher):

    def __init__(self, mq_host):
        Publisher.__init__(self, "RSS Fetch Response Publisher", mq_host,
                           queue_name='RSS_FETCH_RESPONSE', durable=False)


class RssFetcherDaemon(Daemon):

    """Pool of threads consuming tasks from a queue"""

    def __init__(self, num_workers, mq_host, db_config, pid_file, out_file):
        Daemon.__init__(self, pid_file, out_file, out_file, out_file)

        self.num_workers = num_workers
        self.mq_host = mq_host
        self.db_config = db_config

    def run(self):
        try:
            consumer = Consumer("rss-fetcher-consumer", self.mq_host,
                                'RSS_FETCH_QUEUE',
                                {'prefetch_count': self.num_workers})

            drop_publisher = DropPublisher(mq_host)
            response_publisher = ResponsePublisher(mq_host)

            for x in range(self.num_workers):
                RssFetcherWorker("worker-" + str(x), consumer.message_queue,
                                 consumer.confirm_queue, self.db_config,
                                 drop_publisher, response_publisher)

            log.info("Workers started")
            consumer.join()
        except Exception, e:
            #Catch unhandled exceptions
            log.exception(e)
        finally:
            log.info("Exiting")


if __name__ == "__main__":
    config = ConfigParser.SafeConfigParser()
    config.readfp(open(dirname(
        realpath(__file__)) + '/config/rss_fetcher.cfg'))

    try:
        log_file = config.get("main", 'log_file')
        out_file = config.get("main", 'out_file')
        pid_file = config.get("main", 'pid_file')
        num_workers = config.getint("main", 'num_workers')
        log_level = config.get("main", 'log_level')
        mq_host = config.get("main", 'mq_host')

        db_config = {
            'host': config.get("db", 'host'),
            'port': config.getint("db", 'port'),
            'user': config.get("db", 'user'),
            'pass': config.get("db", 'pass'),
            'database': config.get("db", 'database')}

        FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        log.basicConfig(filename=log_file,
                        level=getattr(log, log_level.upper()),
                        format=FORMAT)

        # Create outfile if it does not exist
        file(out_file, 'a')

        daemon = RssFetcherDaemon(num_workers, mq_host, db_config,
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
