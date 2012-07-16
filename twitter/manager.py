#!/usr/bin/env python
# encoding: utf-8

import ConfigParser
import json
import logging as log
import pickle
import socket
import sys
import time
import utils
from os.path import dirname, exists, realpath
from threading import Thread, RLock, Event
from urllib import urlencode

from httplib2 import Http

import MySQLdb

from swiftriver import Daemon, Worker, Consumer, Publisher


class TwitterFirehoseManager(Daemon):
    """ This class manages updates to the track and follow predicates
    which are submitted to the Twitter Firehose via the streaming API

    It has an internal queue that monitors the current no. of predicates
    as per the guidelines specified by Twitter.This class also takes care
    of disconnecting and re-connecting to the Firehose whenever predicates
    are added/deleted
    """

    def __init__(self, pid_file, out_file, mq_host, db_config, num_workers,
                 cache_file, twitter_cache):
        Daemon.__init__(self, pid_file, out_file, out_file, out_file)

        self.__mq_host = mq_host
        self.__db_config = db_config
        self.__lock = RLock()
        self.__db = None

        self.__predicate_workers = num_workers

        self.http = Http()
        # Filter predicates for the firehose
        self.predicates = {}

        # Tracks whether the predicates have changed
        self.predicates_changed = False

        self.cache_file = cache_file

        # In-memory cache for the twitter user ids
        self.twitter_user_ids = twitter_cache

    def get_cursor(self):
        """ Returns a cursor object"""
        cursor = None
        while not cursor:
            try:
                if not self.__db:
                    self.__db = MySQLdb.connect(
                        host=self.__db_config['host'],
                        port=self.__db_config['port'],
                        passwd=self.__db_config['pass'],
                        user=self.__db_config['user'],
                        db=self.__db_config['database'])

                self.__db.ping(True)
                cursor = self.__db.cursor()
            except MySQLdb.OperationalError, e:
                log.error("%s Error connecting to the database. Retrying..." %
                          e)
                time.sleep(60)

        return cursor

    def add_filter_predicate(self, payload):
        """ Adds a filter predicate to the firehose """

        # Grab essential data
        predicate_type = self._get_predicate_type(payload['key'])
        filter_predicate = json.loads(payload['value'])['value']
        river_id = payload['river_id']

        # Get the update target
        update_target = self.predicates.get(predicate_type, dict())
        current_target = dict(update_target)

        # Check for filter predicate limits
        if not utils.allow_filter_predicate(self.predicates, predicate_type):
            log.error("The predicate quota for %s is maxed out." %
                      predicate_type)
            return

        # Proceed
        self._sanitize_filter_predicate(predicate_type, filter_predicate,
                                        update_target, river_id)

        # Update twitter cache
        self._update_twitter_cache()

        # set-to-list conversion
        for k, v in update_target.iteritems():
            update_target[k] = list(v)

        # Update internal list of predicates
        self.predicates[predicate_type] = update_target

        publish_data = {}
        if len(current_target) == 0:
            # Predicate type was non-existent before sanitization,
            # ignore diff compute
            publish_data[predicate_type] = update_target
        else:
            # Predicate type existing prior to sanitization, compute diff
            new_predicates = list(set(update_target.keys()) -
                                  set(current_target.keys()))

            # Check for new predicates
            if len(new_predicates) > 0:
                publish_data[predicate_type] = {}
                k = map(lambda x: dict({x: [river_id]}), new_predicates)
                for v in k:
                    publish_data[predicate_type].update(v)
            elif len(new_predicates) == 0:
                # No new filter predicates, check for rivers update
                # for each predicate
                publish_data[predicate_type] = {}

                for k, v in update_target.iteritems():
                    # Get the rivers tied to the current set of predicates
                    g = current_target[k]
                    rivers_diff = list(set(v) - set(g))
                    if len(rivers_diff) > 0:
                        publish_data[predicate_type].update({k: rivers_diff})

                # Verify that there's data to be published
                if len(publish_data[predicate_type]) == 0:
                    publish_data = {}

        # Check if there's any data to be published
        if len(publish_data) > 0:
            with self.__lock:
                self.predicates_changed = True

    def delete_filter_predicate(self, payload):
        """ Removes a predicate from the internal cache and from
        firehose predicates list"""

        predicate_type = self._get_predicate_type(payload['key'])
        filter_predicate = json.loads(payload['value'])['value']
        river_id = payload['river_id']

        delete_items = {}
        self._sanitize_filter_predicate(predicate_type, filter_predicate,
                                         delete_items, river_id)
        # Update the internal cache
        self._update_twitter_cache()

        # Get the current set of predicates from memory
        current_predicates = self.predicates.get(predicate_type, dict())

        # Nothing to delete
        if len(current_predicates) == 0:
            return

        # Remove the deleted items from predicate internal registry
        for k, v in delete_items.iteritems():
            # Rivers currently using the predicate
            rivers = map(lambda x: int(x), current_predicates.get(k, []))

            # Rivers the predicate has been deleted from
            deleted_rivers = map(lambda x: int(x), v)

            # Get the delta of the two sets of river ids
            delta = list(set(rivers) - set(deleted_rivers))

            if len(delta) == 0 and k in current_predicates:
                # No rivers for that predicate, remove it
                del current_predicates[k]
            else:
                current_predicates[k] = delta

        log.info("Deleted filter predicate %s from river %s" %
                 (filter_predicate, river_id))

        self.predicates[predicate_type].update(current_predicates)
        with self.__lock:
            self.predicates_changed = True

    def _get_predicate_type(self, payload_key):
        """ Given the payload key, returns the predicate type"""

        if payload_key.lower() == "person" or payload_key.lower() == "user":
            return "follow"
        else:
            return "track"

    def _sanitize_filter_predicate(self, predicate_type, filter_predicate,
                                   update_target, river_id):
        """Given a filter predicate, splits it, removes '#' and '@'
        and pushes it to the specified target"""

        for term in filter_predicate.split(","):
            term = term.lower().strip()

            # Strip '@' and '#' off the track predicate
            term = term[1:] if term[:1] == '@' else term
            term = term[1:] if term[:1] == '#' else term

            if predicate_type == 'follow':
                # User lookup via the API
                term = self._get_twitter_user_id(term)

            # As per the streaming API guidelines, terms should be 60 chars
            # long at most
            if  term == False or len(term) > 60:
                continue

            if not term in update_target:
                update_target[term] = set()
            else:
                L = set(update_target[term])
                update_target[term] = L

            # Store the river id with that item
            update_target[term].add(river_id)

    def _get_firehose_predicates(self):
        """Gets all the twitter channel options and classifies them
        as track and follow predicates"""

        c = self.get_cursor()
        c.execute("""
        SELECT river_id, `key`, `value`
        FROM channel_filters cf, channel_filter_options cfo
        WHERE cfo.channel_filter_id = cf.id
        AND cf.channel = 'twitter'
        AND cf.filter_enabled = 1
        """)

        predicates = {}
        for river_id, key, value in c.fetchall():
            predicate_type = self._get_predicate_type(key)

            if not predicate_type in predicates:
                predicates[predicate_type] = {}

            if utils.allow_filter_predicate(predicates, predicate_type):
                update_target = predicates[predicate_type]

                # Get filter predicate and submit it for sanitization
                filter_predicate = json.loads(value)['value']
                self._sanitize_filter_predicate(predicate_type,
                                                filter_predicate,
                                                update_target, river_id)

                predicates[predicate_type] = update_target
            else:
                break

        c.close()

        # Update the twitter cache only if the DB data is different
        # from that in the cache
        if 'follow' in predicates:
            current = set(predicates['follow'].keys())
            cache_data = set(self.twitter_user_ids.keys())
            if (current - cache_data) is not None:
                self._update_twitter_cache()

        # Convert the sets to lists
        for k, v in predicates.iteritems():
            for term, river_ids in v.iteritems():
                predicates[k][term] = list(river_ids)

        return predicates

    def run_manager(self):
        # Options to be passed to the predicate update workers
        while True:
            if self.predicates_changed:
                log.info("Placing filter predicates in the firehose queue %r"
                         % self.predicates)
                self.firehose_publisher.publish(self.predicates)
                with self.__lock:
                    self.predicates_changed = False
            time.sleep(8)

    def run(self):
        log.info("Twitter Firehose Manager started")

        self.firehose_publisher = FirehosePublisher(self.__mq_host)

        # Spawn a set of workers to listen for predicate updates
        options = {'exchange_name': 'chatter',
                   'exchange_type': 'topic',
                   'routing_key': 'web.channel_option.twitter.*',
                   'durable_exchange': True,
                   'prefetch_count': self.__predicate_workers}

        channel_update_consumer = Consumer("channel-update-consumer",
                                           self.__mq_host,
                                           utils.TWITTER_UPDATE_QUEUE,
                                           options)

        for x in range(self.__predicate_workers):
            TwitterPredicateUpdateWorker(
                "twitter-predicate-updater-" + str(x),
                channel_update_consumer.message_queue,
                channel_update_consumer.confirm_queue,
                self)

        # Get all the predicates from the database
        self.predicates = self._get_firehose_predicates()
        self.predicates_changed = (len(self.predicates) > 0)

        self.run_manager()

    def _get_twitter_user_id(self, screen_name):
        """Given a screen name, looks up the ID of the user. Returns, the id
        if found, False otherwise"""

        if screen_name in self.twitter_user_ids:
            return self.twitter_user_ids[screen_name]

        try:
            url = 'http://api.twitter.com/1/users/show.json?screen_name=%s'

            resp, content = self.http.request(url % screen_name, 'GET')
            if resp.status != 200:
                log.error("NOK response (%d) received from the Twitter API" %
                          resp.status)
                return False
            else:
                try:
                    payload = json.loads(content)
                    if 'id_str' in payload:
                        self.twitter_user_ids[screen_name] = payload['id_str']
                        return payload['id_str']
                    else:
                        log.error("Response from Twitter API: %r" % payload)
                except ValueError, error:
                    log.error("Invalid response (%s) from the Twitter API for user %s" %
                              (error, screen_name))
        except socket.error, msg:
            log.error("Error communicating with the Twitter API %s" % msg)

        # If we get here, the lookup failed
        return False

    def _update_twitter_cache(self):
        """Updates the twitter cache on disk"""

        if len(self.twitter_user_ids) > 0:
            output = open(self.cache_file, 'wb')
            pickle.dump(self.twitter_user_ids, output)
            output.close()


class FirehosePublisher(Publisher):

    def __init__(self, mq_host):
        Publisher.__init__(self, "Firehose Publisher", mq_host,
                           queue_name=utils.FIREHOSE_QUEUE, durable=False)


class TwitterPredicateUpdateWorker(Worker):
    """
    Listens for add/delete predicate messages originating the web application
    and updates the internal database/cache
    """

    def __init__(self, name, job_queue, confirm_queue, manager):
        self.manager = manager
        Worker.__init__(self, name, job_queue, confirm_queue)

    def work(self):
        try:
            routing_key, delivery_tag, body = self.job_queue.get(True)
            message = json.loads(body)
            # Add predicates
            if routing_key == "web.channel_option.twitter.add":
                log.info("Add new twitter predicate...")
                self.manager.add_filter_predicate(message)

            # Delete predicates
            if routing_key == "web.channel_option.twitter.delete":
                log.info("Deleting twitter predicate...")

                self.manager.delete_filter_predicate(message)

            self.confirm_queue.put(delivery_tag, False)
        except Exception, e:
            log.info(e)
            log.exception(e)


if __name__ == '__main__':
    # Load the configuration file
    config = ConfigParser.SafeConfigParser()
    config.readfp(open(dirname(realpath(__file__)) + '/config/manager.cfg'))

    try:
        # Setup logging
        log_file = config.get('main', 'log_file')
        out_file = config.get('main', 'out_file')
        log_level = config.get('main', 'log_level')

        FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

        # Load the configuration file
        log.basicConfig(filename=log_file,
                        level=getattr(log, log_level.upper()),
                        format=FORMAT)

        # Create outfile if it does not exist
        file(out_file, 'a')

        # Setup the daemon
        pidfile = config.get('main', 'pid_file')
        stdout = config.get('main', 'out_file')

        # Get the database config
        db_config = {
            'host': config.get("db", 'host'),
            'port': config.getint("db", 'port'),
            'user': config.get("db", 'user'),
            'pass': config.get("db", 'pass'),
            'database': config.get("db", 'database')}

        # Daemon options
        mq_host = config.get('main', 'mq_host')
        num_workers = config.getint('main', 'num_workers')

        # Create the cache file if it doesn't exist
        cache_file = dirname(realpath(__file__)) + "/config/twitter.cache"
        file(cache_file, 'w')

        # Load the list of twitter user ids
        f = open(cache_file, 'rb')
        twitter_cache = pickle.load(f) if len(f.readlines()) > 0 else {}
        f.close()

        # Initialize the daemon
        daemon = TwitterFirehoseManager(pidfile, stdout, mq_host,
                                        db_config, num_workers,
                                        cache_file, twitter_cache)

        if len(sys.argv) == 2:
            if sys.argv[1] == 'start':
                daemon.start()
            elif sys.argv[1] == 'stop':
                daemon.stop()
            elif sys.argv[1] == 'restart':
                daemon.restart()
            else:
                print "Unknown command"
                sys.exit(2)

            sys.exit(0)
        else:
            print "usage %s start|stop|restart" % sys.argv[1]
            sys.exit(2)
    except ConfigParser.NoOptionError, e:
        log.error(" Configuration error:  %s" % e)
