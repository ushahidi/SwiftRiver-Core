#!/usr/bin/env python
# encoding: utf-8

import ConfigParser
import itertools
import json
import logging as log
import re
import rfc822
import socket
import sys
import time
import utils
from multiprocessing import Pool
from os.path import dirname, realpath
from Queue import Queue

from tweepy import OAuthHandler, Stream, StreamListener

from swiftriver import Daemon, DropPublisher, Consumer, Worker


"""
Given a list T with tuples of the form (predicate, rivers, content),
returns the rivers if the predicate is found in the content.
Where the predicate has spaces, it is split and each token matched
against the content
"""


def predicate_match(L):
    # Look for space delimited strings
    search_pattern = L[0].split(" ")

    # Regular expression template for matching predicates
    regex_template = "\\b%s\\b|((\s|^)%s(\s|$))"

    if len(search_pattern) == 1:
        pattern = regex_template % (L[0], L[0])
        return [] if re.search(pattern, L[2], re.IGNORECASE) is None else L[1]
    elif len(search_pattern) > 1:
        # Returns the river ids if each element in the pattern is
        # in the search string
        occurrence = 0

        for p in search_pattern:
            pattern = regex_template % (p, p)
            occurrence += 1 if re.search(pattern, L[2], re.IGNORECASE) else 0

        return L[1] if occurrence >= len(search_pattern) else []


class FilterPredicateMatcher(Worker):
    """ Predicate matching thread"""

    def __init__(self, drop_publisher, predicates, drop_queue,
                 follow_match=False):

        self.drop_publisher = drop_publisher
        self.pool = Pool()

        # Internal predicate registry
        self.predicates = {} if follow_match else []
        self.follow_match = follow_match

        for t in predicates:
            if self.follow_match:
                self.predicates[t[0]] = t[1]
            else:
                self.predicates = predicates

        Worker.__init__(self, "Predicate Matcher", drop_queue, None)

    def work(self):
        start_time, drop = self.job_queue.get(True)
        river_ids = []
        if self.follow_match:
            identity_orig_id = drop['identity_orig_id']
            in_reply_to_user_id = drop['in_reply_to_user_id']
            
            if identity_orig_id in self.predicates:
                river_ids = self.predicates[identity_orig_id]

            if in_reply_to_user_id in self.predicates:
                river_ids += self.predicates[in_reply_to_user_id]
        else:
            predicates = []
            for t in self.predicates:
                item = list(t)
                item.append(drop['droplet_raw'])
                predicates.append(tuple(item))
            
            river_ids = self.pool.map(predicate_match, predicates)

            # Flatten the river ids into a set
            river_ids = list(itertools.chain(*river_ids))
            
        river_ids = list(set(river_ids))
        if len(river_ids) > 0:
            # Log
            log.debug("Droplet content: %s, Rivers: %s Processing Time: %f" %
                      (drop['droplet_content'], river_ids, time.time()-start_time))

            drop['river_id'] = river_ids
            self.drop_publisher.publish(drop)


class TwitterFirehose(Daemon):
    """
    Worker to bootstrap the firehose and handle reconnection when new
    predicates are received
    """

    def __init__(self, pidfile, stdout, options):
        Daemon.__init__(self, pidfile, stdout, stdout, stdout)

        self.mq_host = options.get('mq_host')

        # Track and follow predicates
        self.track = None
        self.follow = None

        track_auth = options.get('track_auth')
        follow_auth = options.get('follow_auth')

        self.auth = {}

        # Verify and load the oAuth params
        if self._verify_auth_params(track_auth):
            self.auth['track'] = self._load_oauth_config(track_auth)

        if self._verify_auth_params(follow_auth):
            self.auth['follow'] = self._load_oauth_config(follow_auth)

        # Internal predicate registry
        self.predicates = {}

        # Firehose streams
        self.track_stream = None
        self.follow_stream = None
        self.reconnect_track_stream = None
        self.reconnect_follow_stream = None

        # Tracks the status of the track and follow firehose streams
        self.track_stream_running = False
        self.follow_stream_running = False

        # Tracks changes to the predicates
        self.track_changed = False
        self.follow_change = False

    def _verify_auth_params(self, params):
        """ Verifies all the OAuth params are present"""
        is_valid = True

        # Verify each of the params is not null and non-zero length
        for param, value in params.iteritems():
            if value is None or len(value.strip()) == 0:
                is_valid = False
                break

        return is_valid

    def _load_oauth_config(self, config):
        """ Returns an OAuth instance from the specified config"""

        auth = OAuthHandler(config.get('consumer_key'),
                            config.get('consumer_secret'))

        # Set the access token
        auth.set_access_token(config.get('token_key'),
                              config.get('token_secret'))
        return auth

    def firehose_reconnect(self):
        """Reconnects to the firehose"""

        t = self._get_filter_predicates(self.predicates)
        self.track, self.follow = t[0], t[1]

        # Track predicates
        if 'track' in self.auth and self.track_changed:
            if self.track is not None and len(self.track) > 0:
                reconnect = self.track_stream_running
                stream = self._init_firehose('track', self.predicates,
                                              reconnect)
                if reconnect:
                    log.info("Updated track predicates: %r" % self.track)
                    self.reconnect_track_stream = stream
                    self.reconnect_track_stream.filter(None, self.track,
                                                       True)
                else:
                    log.info("New track predicates: %r" % self.track)
                    self.track_stream = stream
                    self.track_stream.filter(None, self.track, True)                    
                self.track_stream_running = True
            else:
                log.info("Stopping the track predicates stream")
                self.track_stream.disconnect()
                self.track_stream = None
                self.track_stream_running = False

            self.track_changed = False

        # Follow predicates
        if 'follow' in self.auth and self.follow_changed:
            if self.follow is not None and len(self.follow) > 0:
                reconnect = self.follow_stream_running
                stream = self._init_firehose('follow', self.predicates,
                                              reconnect)
                if reconnect:
                    log.info("Updated follow predicates: %r" % self.follow)
                    self.reconnect_follow_stream = stream
                    self.reconnect_follow_stream.filter(self.follow, None,
                                                        True)
                else:
                    log.info("New follow predicates: %r" % self.follow)
                    self.follow_stream = stream
                    self.follow_stream.filter(self.follow, None, True)
                self.follow_stream_running = True
            else:
                log.info("Stopping the follow predicates stream")
                self.follow_stream.disconnect()
                self.follow_stream = None
                self.follow_stream_running = False

            self.follow_changed = False

    def disconnect_firehose(self, predicate_type):
        """ Given a predicate type , disconnects its current
        firehose stream"""

        log.info("Disconnecting old %s predicate firehose stream" %
                 (predicate_type))

        if predicate_type == 'track':
            self.track_stream.disconnect()
            self.track_stream = self.reconnect_track_stream
            self.reconnect_track_stream = None
        elif predicate_type == 'follow':
            self.follow_stream.disconnect()
            self.follow_stream = self.reconnect_follow_stream
            self.reconnect_follow_stream = None

    def run_firehose(self, message_queue, confirm_queue):
        # Get the items to place on the firehose
        while True:
            method, properties, body = message_queue.get(True)
            predicates = json.loads(body)

            t = [None, None]

            # Update internal predicate registry
            if len(predicates) > 0:
                log.info("Updating streaming API predicates...")

                # Initialize internal predicate registry
                if len(self.predicates) > 0:
                    # Compute diff
                    self._find_predicate_changes(predicates)

                self.predicates = predicates

                # Get the list of keywords to track and people to follow
                t = self._get_filter_predicates(predicates)
            else:
                # Disconnect
                self.track_stream.disconnect()
                self.follow_stream.disconnect()
                self.follow_stream_running = False
                self.track_stream_running = False

            if self.follow_stream_running or self.track_stream_running:
                # Attempt firehose reconnection with the new predicates
                self.firehose_reconnect()
            else:
                # Neither the track nor follow streams are running
                # Check the stream for track predicates
                if not self.track_stream_running and t[0] is not None:
                    self.track_stream = self._init_firehose(
                        'track', predicates)
                    if self.track_stream is not None:
                        log.info("Begin streaming track predicates: %r" %
                                 t[0])
                        self.track = t[0]
                        self.track_stream.filter(None, self.track, True)
                        self.track_stream_running = True

                # Check the stream for follow predicates
                if not self.follow_stream_running and t[1] is not None:
                    self.follow_stream = self._init_firehose(
                        'follow', predicates)
                    if self.follow_stream is not None:
                        log.info("Begin streaming follow predicates: %r" %
                                 t[1])
                        self.follow = t[1]
                        self.follow_stream.filter(self.follow, None, True)
                        self.follow_stream_running = True

            #Acknowledge delivery
            confirm_queue.put(method.delivery_tag, False)

    def _init_firehose(self, predicate_type, predicates, reconnect=False):
        """Initializes a stream listener and its associated firehose
        stream connection"""

        if not predicate_type in self.auth:
            return None

        # Firehose worker reference to be passed to the stream listener
        # Only set if reconnect = True
        firehose_worker = self if reconnect else None

        # Get the track predicates
        track_predicates = {predicate_type: predicates.get(predicate_type)}

        # Listener for the specific predicate
        listener = FirehoseStreamListener(self.drop_publisher,
                                          track_predicates, firehose_worker,
                                          predicate_type)

        auth = self.auth[predicate_type]

        # Tweepy stream
        stream = Stream(auth, listener, secure=True)

        # Check the predicate type
        return stream

    def _get_filter_predicates(self, predicates):
        """Given a dictionary of predicates, returns lists
        of the keywords to track and people to follow via the
        streaming API.
        """
        # UTF-8 housekeeping before placing the predicates on the firehose

        track = predicates.get('track').keys()\
                 if 'track' in predicates else None

        follow = predicates.get('follow').keys()\
                  if 'follow' in predicates else None


        return track, follow

    def _find_predicate_changes(self, predicates):
        """Determines which predicates have changed"""

        # Check if the actual track and follow predicates have changed
        t = self._get_filter_predicates(predicates)
        track, follow = t[0], t[1]

        track_diff, follow_diff = [], []

        if track is not None and self.track is not None:
            track_diff = list(set(track) - set(self.track))

        if follow is not None and self.follow is not None:
            follow_diff = list(set(track) - set(self.follow))

        self.track_changed = len(track_diff) > 0
        self.follow_changed = len(follow_diff) > 0

        # Check if any rivers are no longer using a predicate
        # Predicates may not have changed but a river may have stopped
        # using a track or follow predicate
        for k, v in predicates.iteritems():
            for predicate, rivers in v.iteritems():
                try:
                    current_rivers = self.predicates[k][predicate]
                    diff = list(set(current_rivers) - set(rivers))
                    if len(diff) > 0:
                        if k == 'track':
                            self.track_changed = True
                        elif k == 'follow':
                            self.follow_changed = True
                except KeyError:
                    if k == 'track':
                        self.track_changed = True
                    elif k == 'follow':
                        self.follow_changed = True

    def run(self):
        log.info("Firehose Started")
        self.drop_publisher = DropPublisher(self.mq_host)

        consumer = Consumer("firehose-consumer", self.mq_host,
                            utils.FIREHOSE_QUEUE)

        self.run_firehose(consumer.message_queue, consumer.confirm_queue)


class FirehoseStreamListener(StreamListener):
    """Firehose stream listener for processing incoming firehose data"""

    def __init__(self, drop_publisher, predicates, firehose=None,
                 predicate_type=None):
        StreamListener.__init__(self)

        self.drop_publisher = drop_publisher

        self.firehose = firehose
        self.predicate_type = predicate_type
        self.predicate_dict = dict(predicates)
        
        # Whether to match follow predicates
        self.follow_match = self.predicate_type == 'follow'

        # Flatten the fiter predicates
        self.predicate_list = utils.flatten_filter_predicates(predicates)
        
        self.drop_queue = Queue()
        # Spawn a predicate match worker
        FilterPredicateMatcher(self.drop_publisher,
                               self.predicate_list,
                               self.drop_queue,
                               self.follow_match)

    def on_data(self, data):
        """Called when raw data is received from the connection"""

        try:
            payload = json.loads(data)

            if ('in_reply_to_status_id' in payload and
                'retweeted_status' not in payload):
                if self.firehose is not None:
                    # Disconnect the current firehose connection and kill
                    # reference to the firehose worker thread
                    self.firehose.disconnect_firehose(self.predicate_type)
                    self.firehose = None

                # Twitter uses RFC822 dates, parse them as such
                droplet_date_pub = time.strftime('%Y-%m-%d %H:%M:%S',
                                                 rfc822.parsedate(
                                                    payload['created_at']))

                # Filter out non-standard Twitter RTs
                droplet_content = payload['text'].strip()
                retweet_match = re.findall('^(RT\:?)\s*', droplet_content,
                                           re.I)
                if len(retweet_match) == 0:
                    drop = {
                        'channel': 'twitter',
                        'identity_orig_id': payload['user']['id_str'],
                        'in_reply_to_user_id': payload['in_reply_to_user_id_str'],
                        'identity_name': payload['user']['name'],
                        'identity_username': payload['user']['screen_name'],
                        'identity_avatar': payload['user']['profile_image_url'],
                        'droplet_orig_id': payload['id_str'],
                        'droplet_type': 'original',
                        'droplet_title': droplet_content,
                        'droplet_content': droplet_content,
                        'droplet_raw': droplet_content,
                        'droplet_locale': payload['user']['lang'],
                        'droplet_date_pub': droplet_date_pub}
                    self.drop_queue.put((time.time(), drop), False)

            elif 'delete' in payload:
                status = payload['delete']['status']
                self.on_delete(status['id_str'], status['user_id_str'])
            elif 'limit' in payload:
                track = payload['limit']['track']
                self.on_limit(track)

        except Exception:
            # The data delivered by the streamin API could not be
            # serialized into a JSON object, ignore error
            pass

    def on_delete(self, status_id, user_id):
        """Called when a delete notice arrives for a status"""
        log.info("Delete tweet. droplet_orig_id: %s, identity_orig_id %s" %
                 (status_id, user_id))

    def on_limit(self, track):
        """Called when a limitation notice arrives"""
        log.info("Streaming Rate Limiting: # of rate-limited statuses %s" %
                 track)

    def on_error(self, status_code):
        """Called when a non-200 status code is returned"""
        log.error("Received status %s. Retrying in 10s..." % status_code)
        return True

    def on_timeout(self):
        """Called when stream connection times out"""
        log.info("Firehose has timed out. Will reconnect in 5s...")
        return True


if __name__ == '__main__':
    # Load the configuration file
    config = ConfigParser.SafeConfigParser()
    config.readfp(open(dirname(realpath(__file__)) + '/config/firehose.cfg'))

    try:
        # Setup logging
        log_file = config.get('main', 'log_file')
        out_file = config.get('main', 'out_file')
        log_level = config.get('main', 'log_level')

        FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        log.basicConfig(filename=log_file,
                        level=getattr(log, log_level.upper()),
                        format=FORMAT)

        # Create outfile if it does not exist
        file(out_file, 'a')

        # Setup the daemon
        pidfile = config.get('main', 'pid_file')
        stdout = config.get('main', 'out_file')

        # Daemon options
        options = {
            'mq_host': config.get('main', 'mq_host'),

            # OAuth settings for exclusive use of track predicates
            'track_auth': {
                'consumer_key': config.get('track_twitter_api',
                                           'consumer_key'),
                'consumer_secret': config.get('track_twitter_api',
                                              'consumer_secret'),
                'token_key': config.get('track_twitter_api',
                                        'token_key'),
                'token_secret': config.get('track_twitter_api',
                                           'token_secret')},

            # OAuth settings for exclusive use of follow predicates
            'follow_auth': {
                'consumer_key': config.get('follow_twitter_api',
                                           'consumer_key'),
                'consumer_secret': config.get('follow_twitter_api',
                                              'consumer_secret'),
                'token_key': config.get('follow_twitter_api',
                                        'token_key'),
                'token_secret': config.get('follow_twitter_api',
                                           'token_secret')}}

        # Create the daemon
        daemon = TwitterFirehose(pidfile, stdout, options)

        # Check for the submitted command
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
            print "usage %s start|stop|restart" % sys.argv[0]
            sys.exit(2)
    except ConfigParser.NoOptionError, e:
        log.error(" Configuration error:  %s" % e)
