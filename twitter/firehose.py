import ConfigParser
import itertools
import json
import logging as log
import pika
import re
import sys
import utils
from multiprocessing import Pool
from os.path import dirname, realpath
from swiftriver import Daemon, Worker
from threading import Thread, Event
from tweepy import OAuthHandler, Stream, StreamListener



def predicate_match(L):
    # Look for space delimited strings
    search_pattern = L[0].split(" ")
#    log.debug("\nPattern: %s\n String: %s" % (L[0], L[2]))
        
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
                

class FilterPredicateMatchWorker(Thread):
    
    def __init__(self, channel, predicates, drop_dict):
        Thread.__init__(self)
        self.channel = channel
        self.predicates = []
        search_string = drop_dict['droplet_content']
        for t in predicates:
            item = list(t)
            item.append(search_string)
            self.predicates.append(tuple(item))
        
        self.drop_dict = drop_dict
        
    def run(self):
        # Map reduce
        pool = Pool(processes=5)
        river_ids = pool.map(predicate_match, self.predicates)
        
        # Flatten the river ids into a set
        river_ids = list(itertools.chain(*river_ids))
        if len(river_ids) > 0:
            self.drop_dict['river_id'] = list(river_ids)
            self.channel.queue_declare(queue=Worker.DROPLET_QUEUE, durable=True)
            self.channel.basic_publish(exchange='', 
                                       routing_key=Worker.DROPLET_QUEUE,
                                       properties=pika.BasicProperties(delivery_mode=2),
                                       body=json.dumps(self.drop_dict))
        

class TwitterFirehoseWorker(Worker):
    
    def __init__(self, name, mq_host, queue, options=None):
        Worker.__init__(self, name, mq_host, queue, options)
        
        # Setup oAuth
        self.auth = OAuthHandler(options.get('consumer_key'), 
                                   options.get('consumer_secret'))
        
        self.auth.set_access_token(options.get('token_key'), 
                                     options.get('token_secret'))
        
        self.predicates = []
        
        # Firehose not running
        self.firehose_running = False
    
    def __flatten_filter_predicates(self, predicates):
        """
        Given a dictionary of filter predicates, return a list of tuples
        of predicates and the list of river ids they relate to.
        """
        combined = {}
        for k, v in predicates.iteritems():
            for term, river_ids in v.iteritems():
                try:
                    combined[term].update(set(river_ids))
                except KeyError:
                    combined[term] = set(river_ids)
        
        # Final set - generate the tuples
        for k, v in combined.iteritems():
            self.predicates.append((k, list(v)))
        
        
    def handle_mq_response(self, ch, method, properties, body):
        # Get the items to place on the firehose
        predicates = json.loads(body)
        self.__flatten_filter_predicates(predicates)
        
        log.info("Received filter predicates for the firehose")
        
        # Intialize the firehose stream listener - handles data received
        # from the stream
        stream_listener = FirehoseStreamListener(self.mq, self)
        
        track = (predicates.get('track').keys() 
                 if predicates.has_key('track') else None)
        
        follow = None
#        follow = (predicates.get('follow').keys() 
#                  if predicates.has_key('follow') else None)
        
        # Test filter predicates
#        track = ["Uganda", "International Women's Day", "Kardashian", "iwd"]
#        follow = None
        
        log.debug("Keywords to track %r" % track)
        log.debug("People to follow %r" %follow)
        
        # Initialize a stream listener
        firehose_stream = None
        if not self.firehose_running:
            self.firehose_running = True
            firehose_stream = Stream(self.auth, stream_listener, secure=True)
            
        # Start the firehose filter
        log.info("Initializing the Twitter Streaming API - Filter method")
        firehose_stream.filter(follow, track, True)
        
        #Acknowledge delivery
        ch.basic_ack(delivery_tag = method.delivery_tag)
        

class FirehoseStreamListener(StreamListener):
    
    def __init__(self, mq, firehose_worker):
        StreamListener.__init__(self)
        
        self.channel = mq.channel()
        self.firehose_worker = firehose_worker
        
    def on_data(self, data):
       """Called when raw data is received from the connection"""
       
       if 'in_reply_to_status_id' in data:
           payload = json.loads(data)
           
           drop_dict = {
                        'channel': 'twitter',
                        'identity_orig_id': payload['user']['id_str'],
                        'identity_name': payload['user']['name'],
                        'identity_username': payload['user']['screen_name'],
                        'identity_avatar': payload['user']['profile_image_url'],
                        'droplet_org_id': payload['id_str'],
                        'droplet_type': 'original',
                        'droplet_title': payload['text'],
                        'droplet_content': payload['text'],
                        'droplet_locale': payload['user']['lang'],
                        'droplet_date_pub': payload['created_at']
                        }
           
#           log.debug("Droplet to be mapped %r" %drop_dict)
           
           # Spawn a predicate match worker
           FilterPredicateMatchWorker(self.channel, 
                                      self.firehose_worker.predicates, drop_dict).start()
           

       elif 'delete' in data:
           status = json.loads(data)['delete']['status']
           status_id, user_id = status['id_str'], status['user_id_str']
           self.on_delete(status_id, user_id)
       elif 'limit' in data:
           track = json.loads(data)['limit']['track']
           self.on_limit(track)
       else:
           log.info("Out of sequence API response %s" % data)
       
    
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
        self.__options = options.get('auth')
    
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
        options = {'mq_host': config.get('main', 'mq_host'),
                   'auth': {
                            'consumer_key': config.get('twitter_api', 'consumer_key'),
                            'consumer_secret': config.get('twitter_api', 'consumer_secret'),
                            'token_key': config.get('twitter_api', 'token_key'),
                            'token_secret': config.get('twitter_api', 'token_secret')
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