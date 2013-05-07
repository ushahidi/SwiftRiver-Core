#!/usr/bin/env python
# encoding: utf-8

import ConfigParser
import json
import logging as log
import pickle
import re
import socket
import sys
import time
import utils
from os import stat
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
        self._predicates_changed = False

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

    def add_filter_predicate(self, parameters, river_id, channel_id):
        """ Adds a filter predicate to the firehose """
        has_changed = False
        for key, filter_predicate in parameters.iteritems():
            predicate_type = self._get_predicate_type(key)

            # Get the update target
            update_target = self.predicates.get(predicate_type, dict())
            current_target = dict(update_target)

            # Check for filter predicate limits
            if not utils.allow_filter_predicate(self.predicates, predicate_type):
                log.error("The predicate quota for %s is maxed out." %
                          predicate_type)
                break

            # Proceed
            self._sanitize_filter_predicate(predicate_type,
                filter_predicate, update_target, river_id, channel_id)

            # set-to-list conversion
            for k, v in update_target.iteritems():
                for channel_id, river_id in v.iteritems():
                    update_target[k][channel_id] = river_id

            # Update internal list of predicates
            self.predicates[predicate_type] = update_target
            has_changed = True

        if has_changed:
            with self.__lock:
                self._update_twitter_cache()
                self._predicates_changed = True

    def delete_filter_predicate(self, parameters, river_id, channel_id):
        """ Removes a predicate from the internal cache and from
        firehose predicates list"""
        has_changed = False

        for key, filter_predicate in parameters.iteritems():
            predicate_type = self._get_predicate_type(key)

            delete_items = {}
            self._sanitize_filter_predicate(predicate_type, filter_predicate,
                                            delete_items, river_id,
                                            channel_id)

            # Get the current set of predicates from memory
            current_predicates = self.predicates.get(predicate_type, dict())

            # Nothing to delete
            if len(current_predicates) == 0:
                continue

            # Remove the deleted items from predicate internal registry
            for term, channels in delete_items.iteritems():
                if not current_predicates.has_key(term):
                    continue

                for channel_id in channels.keys():
                    del current_predicates[term][channel_id]

                # Any channels for the current term
                if len(current_predicates.get(term)) == 0:
                    del current_predicates[term]

            self.predicates[predicate_type].update(current_predicates)
            has_changed = True

        if has_changed:
            with self.__lock:
                # Update the internal cache
                self._update_twitter_cache()
                self._predicates_changed = True

    def _get_predicate_type(self, payload_key):
        """ Given the payload key, returns the predicate type"""

        if payload_key.lower() == "person" or payload_key.lower() == "user":
            return "follow"
        else:
            return "track"

    def _sanitize_filter_predicate(self, predicate_type, filter_predicate,
                                   update_target, river_id, channel_id):
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

            # Store the river id against the respective channel
            if not term in update_target:
                update_target[term] = {channel_id: river_id}

    def _get_twitter_predicates(self):
        """Gets all the twitter channel options and classifies them
        as track and follow predicates"""

        c = self.get_cursor()
        c.execute("""
        select rc.id AS channel_id, river_id, parameters
        from rivers r, river_channels rc
        where r.id = rc.river_id
        and rc.channel = 'twitter'
        and rc.active = 1
        """)

        for channel_id, river_id, parameters in c.fetchall():
            parameter_dict = json.loads(parameters)['value'];
            log.info("%s" % parameters)
            self.add_filter_predicate(parameter_dict, river_id, channel_id)

        c.close()

    def run_manager(self):
        # Options to be passed to the predicate update workers
        while True:
            if self._predicates_changed:
                log.info("Placing filter predicates in the firehose queue %r"
                         % self.predicates)
                self.firehose_publisher.publish(self.predicates)
                with self.__lock:
                    self._predicates_changed = False
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
        self._get_twitter_predicates()
        log.info("%r" % self.predicates)
        self._predicates_changed = (len(self.predicates) > 0)

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
            method, properties, body = self.job_queue.get(True)
            message = json.loads(body)
            routing_key = method.routing_key

            # Check for messages updates to the twitter predicates
            if re.search("^web.channel", routing_key, re.I):
                if message['channel'] == 'twitter':
                    channel_id = int(message['id'])
                    river_id = int(message['river_id'])
                    parameters = json.loads(message['parameters'])['value']

                    # Check for operation to be performed - add/delete
                    if routing_key == "web.channel.twitter.add":
                        log.info("Adding channel %d" % channel_id)
                        self.manager.add_filter_predicate(parameters,
                            river_id, channel_id)
                    elif routing_key == "web.channel.twitter.delete":
                        self.manager.delete_filter_predicate(parameters,
                            river_id, channel_id)

                    self.confirm_queue.put(method.delivery_tag, False)
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
        cache_file = config.get('main', 'cache_file')
        if not exists(cache_file):
            f = open(cache_file, 'wb')
            f.close()

        # Load the list of twitter user ids
        f = open(cache_file, 'rb')
        twitter_cache = {} if stat(cache_file)[6] == 0 else pickle.load(f)
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
