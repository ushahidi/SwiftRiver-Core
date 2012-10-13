#!/usr/bin/env python
"""
Pushes drops to the configured Ushahidi deployments. This application listens
for the "web.bucket.push.ushahidi" message on the MQ before initiating a push
to the deployment. Once the drops have been posted to Ushahidi, the push
log is updated

Copyright (c) Ushahidi Inc, 2012. All Rights Reserved
"""

from base64 import b64encode
from datetime import datetime
from os.path import dirname, realpath, basename
from threading import Thread, RLock
from urllib import urlencode
import ConfigParser
import hashlib
import hmac
import json
import logging as log
import sys
import socket
import ssl
import time

from httplib2 import Http
import MySQLdb

from swiftriver import Daemon, Consumer, Worker


class UshahidiPosterDaemon(Daemon):

    def __init__(self, pidfile, outfile, mq_host, num_workers, db_config):
        """Initialize the daemon, properties"""
        Daemon.__init__(self, pidfile, outfile, outfile, outfile)

        self.mq_host = mq_host
        self.num_workers = num_workers
        self._db_config = db_config
        self._db = None
        self._http = Http()

        # Shared lock to control access to the DB by the
        # worker threads
        self._lock = RLock()

    def _get_cursor(self):
        """Connects to the database and returns cursor object to be used
        for DB operations"""
        cursor = None
        config = self._db_config
        while not cursor:
            try:
                if not self._db:
                    self._db = MySQLdb.connect(
                        host=config['host'],
                        port=config['port'],
                        passwd=config['pass'],
                        user=config['user'],
                        db=config['database'])

                self._db.ping(True)
                cursor = self._db.cursor()
            except MySQLdb.OperationalError, e:
                log.error("Error connecting to the database (%s)" % e)
                # Wait for 60s before reconnecting
                time.sleep(60)

        return cursor

    def _get_pending_drops(self):
        """Gets the buckets with drops that are ready for posting
        to Ushahidi. This method is only called once - when the application
        starts up"""
        pass

    def post_drops(self, post_url, drops, bucket_id, client_id,
                   client_secret):
        """Encodes the drops, computes a checksum and posts them to the
        specified URL. This method is invoked when an
        "web.bucket.push.usahidi" message is received on the MQ """
        # Acquire the lock
        with self.__lock:
            # Encode the drops as JSON and run a base64 encode
            drops_payload = b64encode(json.dumps(drops))

            # Generate SHA256 hash_hmac on the payload using client_secret
            # as the key
            h = hmac.new(client_secret, drops_payload, hashlib.sha256())

            # Data to be posted
            post_data = {
                drops: drops_payload,
                checksum: h.hexdigest(),
                client_id: client_id}

            headers = {'Content-type': 'application/x-www-form-urlencoded'}

            response = content = None
            while not response:
                try:
                    # Post the drops -  HTTP post
                    response, content = self._http.request(
                        post_url, 'POST', body=urlencode(post_data),
                        headers=headers)

                    # If HTTP 200 status, update the push log for the bucket
                    if response.status == 200:
                        log.info("Drops successfully posted to Ushahidi")
                        # Update the push log for the bucket
                        self._update_push_log(bucket_id, drops)
                    else:
                        log.error("API returned status %s. Retrying" %
                            response.status)

                        # Reset the response and the content
                        response = content = None
                        # Wait 30s before retrying
                        time.sleep(30)
                except socket.error, e:
                    log.error("Error posting drops to Ushahidi (%s)" % e)

    def _update_push_log(self, bucket_id, drops):
        """Updates the push log for the specified bucket by setting
        droplet_push_status = 1 for each of the drops"""

        log.info("Updating the deployment push log")

        query_template = "SELECT %d AS `bucket_id` %d AS `droplet_id`"
        queries = []
        for drop in drops:
            queries.append(query_template % (bucket_id, drop['id']))

        # Query to update push log
        update_query = """UPDATE `deployment_push_logs` AS a
        JOIN (%s) AS b ON (b.bucket_id = a.bucket_id)
        SET a.droplet_push_status = 1, a.droplet_push_date = '%s'
        WHERE b.droplet_id = a.droplet_id """

        # Get the UTC date
        push_date = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        update_query = update_query % (' UNION ALL '.join(queries), push_date)

        cursor = self._get_cursor()

        # Execute the query, commit the transaction and close the cursor
        cursor.execute(update_query)
        self._db.commit()
        cursor.close()

        log.info("Deployment push log successfully updated")

    def run(self):
        """Initializes the daemon and spawns s set of workers to listen
        on the MQ for buckets that are ready to push drops"""

        options = {'exchange_name': 'chatter',
                   'exchange_type': 'topic',
                   'routing_key': 'web.bucket.push.ushahidi.*',
                   'durable_exchange': True}

        # Consumer for USHAHIDI_POST_QUEUE
        postqueue_consumer = Consumer(
            "ushahidi-postqueue-consumer",
            self.mq_host,
            "USHAHIDI_POST_QUEUE",
            options)

        # Spawn a set of workers to listen for buckets that are ready
        # to post drops
        for x in range(self.num_workers):
            UshahidiPostQueueWorker(
                "ushahidi-postqueue-worker" + str(x),
                postqueue_consumer.message_queue,
                postqueue_consumer.confirm_queue,
                self)

        log.info("Workers started")

        postqueue_consumer.join()
        log.info("Exiting...")


class UshahidiPostQueueWorker(Worker):
    """Worker thread to listen for and fetch "web.bucket.push.ushahidi"
    messages from the MQ"""

    def __init__(self, name, job_queue, confirm_queue, poster):
        Worker.__init__(self, name, job_queue, confirm_queue)

        # Poster is an instance of UshahidiPosterDaemon
        self._poster = poster

    def work(self):
        try:
            method, properties, body = self.job_queue.get(True)
            # Decode the JSON
            message = json.loads(body)
            # Check the routing key
            if method.routing_key == "web.bucket.push.ushahidi":
                # Post the drops
                log.info("Posting drops for bucket %s" % message['bucket_id'])
                self._poster.post_drops(
                    message['post_url'], message['drops'],
                    message['bucket_id'], message['client_id'],
                    message['client_secret'])
        except Exception, e:
            log.error(e)


if __name__ == '__main__':
    # Load the configuration file
    config = ConfigParser.SafeConfigParser()
    config.readfp(open(dirname(
        realpath(__file__)) + '/config/ushahidi.cfg'))

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

        daemon = UshahidiPosterDaemon(pid_file, out_file, mq_host,
                                      num_workers, db_config)

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
        log.error("Configuration error:  %s" % e)
