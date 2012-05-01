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

import sys
import time
import logging as log
import pika
import feedparser
import json
import ConfigParser
import pickle
import hashlib
from threading import Event
from os.path import dirname, realpath
from swiftriver import Daemon, Worker


class RssFetcherWorker(Worker):
    """Thread executing tasks from a given queue"""

    SHEDULER_RESPONSE_QUEUE = 'RSS_FETCH_RESPONSE'

    def __init__(self, name, mq_host, queue, options=None):
        Worker.__init__(self, name, mq_host, queue, options)

    def handle_mq_response(self, ch, method, properties, body):
        """Process a URL"""
        message = json.loads(body)
        log.info("%s fetching %s" % (self.name, message['url']))

        # The droplet queue
        droplet_channel = self.mq.channel()
        droplet_channel.queue_declare(queue=self.DROPLET_QUEUE, durable=True)
        drops = []

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
                drop = pickle.loads(db_drop)
                drop['river_id'] = message['river_ids']
                droplet_channel.basic_publish(
                    exchange='', routing_key=self.DROPLET_QUEUE,
                    properties=pika.BasicProperties(
                        delivery_mode=2, # make message persistent
                    ),
                    body=json.dumps(drop))

            c.close()

        if not (message.get('use_cache', False) and cache_found):
            # Fetch the feed and use etag/modified headers if they
            # were provided in the last fetch
            d = None
            if message['last_fetch_etag']:
                d = feedparser.parse(message['url'],
                                     etag=message['last_fetch_etag'])
            elif message['last_fetch_modified']:
                d = feedparser.parse(message['url'],
                                     modified=message['last_fetch_modified'])
            else:
                d = feedparser.parse(message['url'])

            # Get the avatar, locale and identity name
            avatar = (d.feed.image.get('href', None)
                      if 'image' in d.feed else None)

            locale = d.feed.get('language', None)
            identity_name = d.feed.get('title', message['url'])

            for entry in d.entries:
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
                drop = {
                    'channel': 'rss',
                    'river_id': message['river_ids'],
                    'identity_orig_id': message['url'],
                    'identity_username' : d.feed.get('link', message['url']),
                    'identity_name': identity_name,
                    'identity_avatar': avatar,
                    'droplet_orig_id' : hashlib.sha256(
                                            entry.get('link', '') +
                                            entry.get('id', '')).hexdigest(),
                    'droplet_type': 'original',
                    'droplet_title': entry.get('title', None),
                    'droplet_content': content,
                    'droplet_locale': locale,
                    'droplet_date_pub': time.strftime('%Y-%m-%d %H:%M:%S',
                                                      entry.get('date_parsed',
                                                                time.gmtime()))
                }

                #log.debug("Drop: %r" % drop)
                drops.append((message['url'],
                              time.mktime(entry.get('date_parsed',
                                                    time.gmtime())),
                              pickle.dumps(drop)))

                droplet_channel.basic_publish(
                    exchange='', routing_key=self.DROPLET_QUEUE,
                    properties=pika.BasicProperties(delivery_mode=2),
                    body=json.dumps(drop))

            # Send the status, etag & modified back the the scheduler
            response_channel = self.mq.channel()
            response_channel.queue_declare(queue=self.SHEDULER_RESPONSE_QUEUE,
                                            durable=False)

            message_body = json.dumps({
                'url': message['url'],
                'status': d.get('status', None),
                'last_fetch_modified': d.get('modified',
                                             message['last_fetch_modified']),
                'last_fetch_etag': d.get('etag', message['last_fetch_etag']),
                'last_fetch_time': int(time.mktime(time.gmtime()))
            })

            response_channel.basic_publish(
                exchange='',
                routing_key=self.SHEDULER_RESPONSE_QUEUE,
                body=message_body)

            response_channel.close()

        droplet_channel.close()
        ch.basic_ack(delivery_tag=method.delivery_tag)
        log.info(" %s done fetching %s" % (self.name, message['url']))

        # Add the drops to our cache
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
                """, % (message['url'], message['url']))

            c.executemany(
                """
                insert into `rss_cache` (`url`, `date_pub`, `drop`)
                values (%s, %s, %s)
                """, drops[:50])

            c.close()
            self.db.commit()


class RssFetcherDaemon(Daemon):

    """Pool of threads consuming tasks from a queue"""

    def __init__(self, num_workers, mq_host, db_config, pid_file, out_file):
        Daemon.__init__(self, pid_file, out_file, out_file, out_file)

        self.num_workers = num_workers
        self.mq_host = mq_host
        self.db_config = db_config

    def run(self):
        try:
            event = Event()

            queue_name = 'RSS_FETCH_QUEUE'
            options = {'db_config': self.db_config}

            for x in range(self.num_workers):
                RssFetcherWorker("worker-" + str(x), self.mq_host,
                                 queue_name, options).start()

            log.info("Workers started")
            event.wait()
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
