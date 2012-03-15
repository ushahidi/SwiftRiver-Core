#!/usr/bin/env python
# encoding: utf-8

import ConfigParser
import itertools
import json
import logging as log
import pika
import re
import rfc822
import socket
import sys
import time
import utils
from multiprocessing import Pool
from os.path import dirname, realpath
from swiftriver import Daemon, Worker
from threading import Thread, Event
from tweepy import OAuthHandler, Stream, StreamListener


"""
Given a list T with tuples of the form (predicate, rivers, content), 
returns the rivers if the predicate is found in the content.
Where the predicate has spaces, it is split and each token matched
against the content
"""
def predicate_match(L):
    # Look for space delimited strings
    search_pattern = L[0].split(" ")
        
    if len(search_pattern) == 1:
        return [] if re.search(L[0], L[2], re.IGNORECASE) is None else L[1]
    elif len(search_pattern) > 1:
        # Returns the river ids if each element in the pattern is 
        # in the search string
        
        found = False
        
        for pattern in search_pattern:
            found = (False if re.search(pattern, L[2], re.IGNORECASE) 
                     is None else True)
        
        return L[1] if found else []
                

class FilterPredicateMatcher(Thread):
    """ Predicate matching thread"""
    
    def __init__(self, mq_host, predicates, drop_dict):
        Thread.__init__(self)
        
        # Internal predicate registry
        self.predicates = []
        self._channel = None
        
        # Attempt to get a connection
        try:
            params = pika.ConnectionParameters(host=mq_host)
            strategy = pika.reconnection_strategies.SimpleReconnectionStrategy()
                    
            connection = pika.BlockingConnection(params, strategy)
                    
            self._channel = connection.channel()
            self._channel.queue_declare(queue=Worker.DROPLET_QUEUE, durable=True)
        except socket.error, msg:
            log.error("Error connecting predicate matcher to the MQ. %s" % msg)
        except pika.exceptions.ChannelClosed, e:
            log.error("Predicate matcher lost connection to the MQ, reconnecting")
        
        search_string = drop_dict['droplet_content']
        for t in predicates:
            item = list(t)
            item.append(search_string)
            self.predicates.append(tuple(item))
        
        self.drop_dict = dict(drop_dict)
        
    def run(self):
        # Get a pool of processes as many as the cores in the system
        if self._channel is None:
            return
        
        pool = Pool()
        river_ids = pool.map(predicate_match, self.predicates)
        
        # Terminate the workers
        pool.close()
        pool.join()
        
        # Just to be sure
        pool.terminate()
        
        # Flatten the river ids into a set
        river_ids = list(itertools.chain(*river_ids))
        river_ids = list(set(river_ids))
        if len(river_ids) > 0:
            # Log
            log.debug("Droplet content: %s, Rivers: %s" 
                      % (self.drop_dict['droplet_content'], river_ids))
            
            self.drop_dict['river_id'] = river_ids
                
            self._channel.basic_publish(exchange='', 
                                       routing_key=Worker.DROPLET_QUEUE,
                                       properties=pika.BasicProperties(delivery_mode=2),
                                       body=json.dumps(self.drop_dict))
        
        # Close the channel and connection
        self._channel.close()
    
        
class TwitterFirehoseWorker(Worker):
    """
    Worker to bootstrap the firehose and handle reconnection when new 
    predicates are received
    """
    
    def __init__(self, name, mq_host, queue, options=None):
        Worker.__init__(self, name, mq_host, queue, options)
        
        # Track and follow predicates
        self.track = None
        self.follow = None
        
        track_auth = options.get('track_auth')
        follow_auth = options.get('follow_auth')
        
        self.auth = {}

        # Verify and load the oAuth params        
        if self.__verify_auth_params(track_auth):
            self.auth['track'] = self.__load_auth_config(track_auth)
        
        if self.__verify_auth_params(follow_auth):
            self.auth['follow'] = self.__load_auth_config(follow_auth)        
        
        # Internal predicate registry
        self.predicates = {}
        
        self.listeners = {'track': None, 'follow': None}
        self.streams = {'track': None, 'follow': None}
        
        # Tracks the status of the track and follow firehose streams
        self.track_firehose_running = False
        self.follow_firehose_running = False
        
        # Firehose reconnection stream and listener references  
        self.__reconnect_streams = {'track': None, 'follow': None}
        self.__reconnect_listeners = {'track': None, 'follow': None}
    
    def __verify_auth_params(self, params):
        """ Verifies all the OAuth params are present"""
        if (params.get('consumer_key') is not None 
            and params.get('consumer_secret') is not None
            and params.get('token_key') is not None
            and params.get('token_secret') is not None
            ):
            # Success! All parameters present
            return True
        
        return False
    
    def __load_auth_config(self, config):
        """ Returns an OAuth instance from the secified config"""
        
        auth = OAuthHandler(config.get('consumer_key'), 
                            config.get('consumer_secret'))
        
        # Set the access token
        auth.set_access_token(config.get('token_key'), 
                              config.get('token_secret'))
        return auth;
        
    def firehose_reconnect(self):
        """Reconnects to the firehose"""
        
        t = self.__get_filter_predicates(self.predicates)
        self.track, self.follow = t[0], t[1]

        if self.track is not None and self.auth.has_key('track'):
            log.info("Reconnecting with updated track predicates: %r" % t[0])

            if self.__init_firehose('track', self.predicates, True):
                track_stream = self.__reconnect_streams['track']
                track_stream.filter(None, self.track, True)

        if self.follow is not None and self.auth.has_key('follow'):
            log.info("Reconnecting with updated follow predicates: %r" % t[1])

            if self.__init_firehose('follow', self.predicates, True):
                follow_stream = self.__reconnect_streams['follow']
                follow_stream.filter(self.follow, None, True)
        
        
    def disconnect_firehose(self, predicate_type):
        """ Given a predicate type , disconnects its current 
        firehose stream"""
        
        log.info("Disconnecting old %s predicate firehose stream" 
                 % predicate_type)
        
        self.streams[predicate_type].disconnect()
        
        # Set the active streams and listeners
        self.listeners[predicate_type] = self.__reconnect_listeners[predicate_type]
        self.streams[predicate_type] = self.__reconnect_streams[predicate_type]
        
        # Destroy the reconnect references
        self.__reconnect_listeners[predicate_type] = None
        self.__reconnect_streams[predicate_type] = None
        
            
    def handle_mq_response(self, ch, method, properties, body):
        """Overrides Worker.handle_mq_response"""
        
        # Get the items to place on the firehose
        predicates = json.loads(body)
        log.info("Received filter predicates for the firehose")

        t = ()
        
        # Check for existing predicates
        if len(self.predicates) == 0:
            # Initialize internal predicate registry
            self.predicates = dict(predicates)
            
            # Get the list of keywords to track and people to follow
            t = self.__get_filter_predicates(predicates)
        

        # Check the stream for track predicates
        if not self.track_firehose_running and t[0] is not None:
            
            if self.__init_firehose('track', predicates):
                log.info("Initializing streaming of track predicates: %r" % t[0])
                
                self.track_firehose_running = True

                self.track = t[0]
                track_stream = self.streams['track']
                track_stream.filter(None, self.track, True)


        # Check the stream for follow predicates        
        if not self.follow_firehose_running and t[1] is not None:

            if self.__init_firehose('follow', predicates):
                log.info("Initializing streaming of follow predicates: %r" % t[1])
                
                self.follow_firehose_running = True
                
                self.follow = t[1]
                follow_stream = self.streams['follow']
                follow_stream.filter(self.follow, None, True)
        
        # If either of the streams is running, update predicates
        if self.follow_firehose_running or self.track_firehose_running:
            # Update the filter predicates
            self.__update_filter_predicates(predicates)
        
        #Acknowledge delivery
        ch.basic_ack(delivery_tag = method.delivery_tag)
    
    
    def __init_firehose(self, predicate_type, predicates, reconnect=False):
        """Initializes a stream listener and its associated firehose
        stream connection"""
        
        if not self.auth.has_key(predicate_type):
            return False
        
        # Firehose worker reference to be passed to the stream listener
        # Only set if reconnect = True
        firehose_worker = self if reconnect else None
        
        # Get the track predicates
        track_predicates = {predicate_type: predicates.get(predicate_type)}
        
        # Listener for the specific predicate
        listener = FirehoseStreamListener(self.mq_host, track_predicates, 
                                          firehose_worker, predicate_type)
        
        auth = self.auth[predicate_type]
        # Stream for the predicate type
        stream = Stream(auth, listener, secure=True)

        if reconnect:
            self.__reconnect_listeners[predicate_type] = listener
            self.__reconnect_streams[predicate_type] = stream
        else:
            self.listeners[predicate_type] = listener
            self.streams[predicate_type] = stream
        
        return True
               
        
    def __update_filter_predicates(self, predicates):
        """Gets the diff between the current set of predicates
        and the newly submitted set  
        """
        # Get the new follow and track predicates
        t = self.__get_filter_predicates(predicates)

        track = None if t[0] is None else t[0]
        follow = None if t[1] is None else t[1]
        
        # Compute the follow diff
        follow_diff = []
        if follow is not None:
             follow_diff = follow if self.follow is None else list(set(follow) - set(self.follow))

        # Computer the track diff
        track_diff = []
        if track is not None:
             track_diff = track if self.track is None else list(set(track) - set(self.track))
        
        if len(track_diff) > 0 or len(follow_diff) > 0:
            # Update the list of predicates 
            for k, v in predicates.iteritems():
                for p, r in v.iteritems():
                    try:
                        d = self.predicates[k][p]
                        self.predicates[k][p] = d.extend(r)
                    except KeyError:
                        self.predicates[k] = {p: r}
            
            # Reconnect to the firehose with the updated predicates
            self.firehose_reconnect()
        elif len(track_diff) == 0 and len(follow_diff) == 0:
            # Check for river id updates
            pass
        
    def __get_filter_predicates(self, predicates):
        """Given a dictionary of predicates, returns lists
        of the keywords to track and people to follow via the
        streaming API.
        """ 
        track = (predicates.get('track').keys() 
                 if predicates.has_key('track') else None)
        
        follow = (predicates.get('follow').keys() 
                  if predicates.has_key('follow') else None)
        
        return track, follow        


class FirehoseStreamListener(StreamListener):
    """Firehose stream listener for processing incoming firehose data"""
    
    def __init__(self, mq_host, predicates, firehose_worker=None, predicate_type=None):
        StreamListener.__init__(self)
        
        self.mq_host = mq_host
        
        self.__firehose_worker = firehose_worker
        self.__predicate_type = predicate_type
        
        self.__predicate_dict = dict(predicates)
        
        # Flatten the fiter predicates
        self.__predicate_list = utils.flatten_filter_predicates(predicates)
   
    def update_predicate_river_ids(self, updated):
        """Given a dictionary of predicates, determines which predicates
        need to be updated with new rivers. Results in the modification
        of the internal predicate list  
        """
        
        # NOTE: Duplicate river_ids will be filtered out by the flattening step
        for k, v in updated.iteritems():
            for p, r in v.iteritems():
                # Compute diff of river ids and extend by the result
                try:
                    current_list = self.__predicate_dict[k][p]
                    current_list.extend(list(set(r) - set(current_list)))
                    self.__predicate_dict[k][p] = current_list
                except KeyError:
                    self.__predicate_dict[k] = {p: r}
        
        # Update the list
        self.__predicate_list = utils.flatten_filter_predicates(self.__predicate_dict)
            
    def on_data(self, data):
       """Called when raw data is received from the connection"""
       
       if 'in_reply_to_status_id' in data:
           if self.__firehose_worker is not None:
               # Disconnect the current firehose connection and kill
               # reference to the firehose worker thread
               log.info("Disconnecting current firehose connection...")
               self.__firehose_worker.disconnect_firehose(self.__predicate_type)
               self.__firehose_worker = None
           
           payload = json.loads(data)
           
           # Twitter appears to be using RFC822 dates, parse them as such 
           drop_dict = {
                        'channel': 'twitter',
                        'identity_orig_id': payload['user']['id_str'],
                        'identity_name': payload['user']['name'],
                        'identity_username': payload['user']['screen_name'],
                        'identity_avatar': payload['user']['profile_image_url'],
                        'droplet_orig_id': payload['id_str'],
                        'droplet_type': 'original',
                        'droplet_title': payload['text'],
                        'droplet_content': payload['text'],
                        'droplet_locale': payload['user']['lang'],
                        'droplet_date_pub': time.strftime('%Y-%m-%d %H:%M:%S', 
                                                          rfc822.parsedate(payload['created_at']))
                        }
           
           # Spawn a predicate match worker
           FilterPredicateMatcher(self.mq_host, 
                                  self.__predicate_list, 
                                  drop_dict).start()
           
       elif 'delete' in data:
           status = json.loads(data)['delete']['status']
           status_id, user_id = status['id_str'], status['user_id_str']
           self.on_delete(status_id, user_id)
       elif 'limit' in data:
           track = json.loads(data)['limit']['track']
           self.on_limit(track)
       else:
           # Out of sequence response, pass
           pass
       
    
    def on_status(self, status):
        """Called when a new status arrives"""
        pass

    def on_delete(self, status_id, user_id):
        """Called when a delete notice arrives for a status"""
        log.info("Delete Twitter droplet. droplet_orig_id: %s, identity_orig_id %s" 
                 % (status_id, user_id))

    def on_limit(self, track):
        """Called when a limitation notice arrives"""
        log.info("Streaming Rate Limiting: # of rate-limited statuses %s" % track)

    def on_error(self, status_code):
        """Called when a non-200 status code is returned"""
        return False

    def on_timeout(self):
        """Called when stream connection times out"""
        return

        

class TwitterFirehoseDaemon(Daemon):
    
    def __init__(self, pidfile, stdout, options):
        Daemon.__init__(self, pidfile, stdout, stdout, stdout)
        
        self.__mq_host = options.get('mq_host')
        
        # Auth settings for the track and filter predicates respectively
        self.__options = {
                          'track_auth':options.get('track_auth'), 
                          'follow_auth':options.get('follow_auth')
                          }
        
    def run(self):
        try:
            event = Event()
            TwitterFirehoseWorker('twitter-firehose', self.__mq_host, 
                                  utils.FIREHOSE_QUEUE, self.__options).start()
            event.wait()
        except Exception, e:
            log.exception(e)
        finally:
            log.info("Exiting")
    

if __name__ == '__main__':
    # Load the configuration file
    config = ConfigParser.SafeConfigParser()
    config.readfp(open(dirname(realpath(__file__))+'/config/firehose.cfg'))
    
    try:
        # Setup logging
        log_file = config.get('main', 'log_file')
        out_file = config.get('main', 'out_file')
        log_level = config.get('main', 'log_level')
        
        FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        log.basicConfig(filename=log_file, level=getattr(log, log_level.upper()), format=FORMAT)
        
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
                            'consumer_key': config.get('track_twitter_api', 'consumer_key'),
                            'consumer_secret': config.get('track_twitter_api', 'consumer_secret'),
                            'token_key': config.get('track_twitter_api', 'token_key'),
                            'token_secret': config.get('track_twitter_api', 'token_secret')
                            },
                    
                    # OAuth settings for exclusive use of follow predicates
                   'follow_auth': {
                            'consumer_key': config.get('follow_twitter_api', 'consumer_key'),
                            'consumer_secret': config.get('follow_twitter_api', 'consumer_secret'),
                            'token_key': config.get('follow_twitter_api', 'token_key'),
                            'token_secret': config.get('follow_twitter_api', 'token_secret')
                            }
                   }
        
        # Create the daemon
        daemon = TwitterFirehoseDaemon(pidfile, stdout, options)
        
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