#!/usr/bin/env python
"""
Copyright (c) 2012 Ushahidi Inc

This script is designed to be run as a cron job.
Its purpose is to check for rivers that are about to expire or those
that have already expired and thereafter, notify their respective
owners via email of the action they need to take

For rivers that have expired, channel delete commands are sent to the
MQ so that they can be removed from the content fetch schedule
"""

import ConfigParser
import hashlib
import json
import logging as log
import random
import string
import sys
import time
from datetime import datetime, timedelta
from os import popen
from os.path import dirname, realpath
from threading import Thread

import MySQLdb
import pika

# Location of the sendmail program
SENDMAIL = ""


class RiverExpiry(object):

    def __init__(self, mq_host, db_config, email_config):
        self.mq_host = mq_host
        self.db_config = db_config
        self.email_config = email_config
        self.db_connection = None
        self.app_settings = None
        self.date_format = '%Y-%m-%d %H:%M:%S'
        self.email_from = None
        self.notice_template = None
        self.warning_template = None

    def start(self):
        """Runs the expiry checks"""
        Thread(target=self._run).start()

    def _run(self):
        """Gets the rivers that have either expired or about to expire,
        sends out notifications to their owners and for the expired
        rivers sends channel delete commands to the MQ"""

        log.info("Starting maintenance")

        # Load the app settings
        self.app_settings = self._get_app_settings()
        notice_period = int(self.app_settings['river_expiry_notice_period'])

        # Expiry notice template
        notice_file = open(self.email_config['expiry_notice'], 'r')
        self.notice_template = notice_file.read()
        notice_file.close()

        # Expiry warning template
        warning_file = open(self.email_config['expiry_warning'], 'r')
        self.warning_template = warning_file.read()
        warning_file.close()

        # Email address for sending out emails
        self.email_from = "%s <%s>" % (self.app_settings['site_name'],
                                       self.email_config['site_email'])

        # Get the current date+time
        current_date = datetime.now()

        # Generate the filter date
        filter_date_obj = current_date + timedelta(days=notice_period)
        filter_date = time.strftime(self.date_format,
                                    filter_date_obj.timetuple())

        # Get rivers eligible for expiry
        rivers = self._get_eligible_rivers(filter_date)

        if len(rivers) == 0:
            log.info("No eligible rivers found. Shutting down")
            self.db_connection.close()
            return

        # For each river, get the days to expiry
        expired_rivers = {}
        candidates = []
        for river_id, data in rivers.iteritems():
            expiry_date = data['expiry_date']
            days_to_expiry = self._days2expiry(expiry_date, current_date)
            rivers[river_id]['days_to_expiry'] = days_to_expiry

            if days_to_expiry == 0:
                # Generate token
                choice_range = string.letters + string.digits
                token_str = ''.join(
                    random.choice(choice_range) for x in range(32))
                token = hashlib.sha256(token_str).hexdigest()

                # Save the token and modify the URL
                expired_rivers[river_id] = token
                url = rivers[river_id]['url']
                rivers[river_id]['url'] = url + '/extend?token=' + token
            else:
                if days_to_expiry > 0 and data['is_candidate'] == 0:
                    candidates.append(river_id)
                else:
                    del rivers[river_id]

        # Shutdown expired rivers
        if len(expired_rivers) > 0:
            self._expire_rivers(expired_rivers)

        # Set the expiry_candidate flag
        if len(candidates) > 0:
            self._set_expiry_candidate_flag(candidates)

        # Fetch the owners
        river_owners = self._get_river_owners(rivers.keys())

        # Send email notifications
        for river_id, data in rivers.iteritems():
            self._notify_owners(river_owners[river_id], data)

        # Close the DB connection
        log.info("Maintenance complete. Shutting DB and SMTP connections")
        self.db_connection.close()

    def _get_db_cursor(self):
        """Initiates a connection to the DB and returns a cursor
        to be used for executing queries"""
        cursor = None
        while not cursor:
            try:
                if not self.db_connection:
                    self.db_connection = MySQLdb.connect(
                        host=self.db_config['host'],
                        port=self.db_config['port'],
                        passwd=self.db_config['pass'],
                        user=self.db_config['user'],
                        db=self.db_config['database'])

                self.db_connection.ping(True)
                cursor = self.db_connection.cursor()
            except MySQLdb.OperationalError, e:
                log.error(
                    "%s Error connecting to the database. Retrying..." %
                    e)
                time.sleep(60)
        return cursor

    def _get_app_settings(self):
        """Gets the settings parameters relevant to river expiry i.e.
        river_active_duration, river_expiry_notice_period, site_url
        and site_name
        """
        app_settings = {}

        # Setttings keys to fetch
        settings_keys = [
            "'river_active_duration'",
            "'river_expiry_notice_period'",
            "'site_name'",
            "'site_url'"]

        key_str = ', '.join(settings_keys)
        cursor = self._get_db_cursor()
        cursor.execute("""
            SELECT `key`, `value` FROM settings
            WHERE `key` IN (%s)""" % key_str)

        for key, value in cursor.fetchall():
            app_settings[key] = value
        cursor.close()

        return app_settings

    def _get_eligible_rivers(self, filter_date):
        """Gets the rivers that are eligible for expiry.
        These are rivers which have the expiry_candidate flag switched on
        or their expiry date is less than or equal to the specified
        filter date"""
        cursor = self._get_db_cursor()
        cursor.execute("""
            SELECT rivers.id AS river_id, rivers.river_name,
            rivers.expiry_candidate, rivers.river_date_expiry,
            CONCAT(accounts.account_path, '/',
                   rivers.river_name_url) AS river_url
            FROM rivers
            INNER JOIN accounts ON (rivers.account_id = accounts.id)
            WHERE rivers.river_expired = 0
            AND rivers.river_date_expiry <= '%s'
            """ % filter_date)

        # For each river, get owners and send out emails to owners
        base_url = self.app_settings['site_url']
        rivers = {}
        for river_id, river_name, expiry_candidate, river_date_expiry,\
            river_url in cursor.fetchall():

             # Store each river and its metadata
            rivers[str(river_id)] = {
                'river_name': river_name,
                'expiry_date': river_date_expiry,
                'days_to_expiry': None,
                'is_candidate': expiry_candidate,
                'url': base_url + '/' + river_url}

        # Close the cursor
        cursor.close()
        return rivers

    def _get_river_owners(self, river_ids):
        """Returns a dictionary of each river and a list of its owners
        The owner data is comprised of their name and email address
        """

        river_ids_str = ', '.join(map(str, river_ids))

        # Get cursor reference and run the query
        cursor = self._get_db_cursor()
        cursor.execute("""
            SELECT river_collaborators.river_id, users.name, users.email 
            FROM users 
            INNER JOIN river_collaborators ON 
                (river_collaborators.user_id = users.id) 
            WHERE river_collaborators.collaborator_active = 1 
            AND river_collaborators.river_id IN (%s) 
            UNION ALL 
            SELECT rivers.id AS river_id, users.name, users.email 
            FROM users 
            INNER JOIN accounts ON (accounts.user_id = users.id) 
            INNER JOIN rivers ON (rivers.account_id = accounts.id) 
            WHERE rivers.id IN (%s)
            """ % (river_ids_str, river_ids_str))

        river_owners = {}
        for river_id, name, email in cursor.fetchall():
            river_id_str = str(river_id)
            if not river_id in river_owners:
                river_owners[river_id_str] = []
            river_owners[river_id_str].append({'name': name, 'email': email})

        # Close the cursor
        cursor.close()
        return river_owners

    def _days2expiry(self, expiry_date, current_date):
        """Given the expiry date and the current date, returns the no. of
        days remaining before the river expires.
        If the current_date >= expiry_date, return 0"""

        if current_date >= expiry_date:
            return 0

        date_diff = current_date - expiry_date
        return date_diff.days

    def _expire_rivers(self, rivers):
        """Notifies the content fetch applications to purge the channel
        options for the specified rivers"""

        log.info("Setting the river_expired flag to 1...")

        # Flatten the river_ids list to a string separated by ","
        river_ids_str = ', '.join(map(str, rivers.keys()))

        # Query for setting the expiry token
        token_update_query = """
            UPDATE rivers
            SET river_expired = 1,
            expiry_extension_token = CASE `id` """

        for river_id, token in rivers.iteritems():
            token_update_query += """WHEN %s THEN '%s' """ % (river_id, token)

        token_update_query += "END WHERE `id` IN (%s)" % river_ids_str

        # Switch on the expired flag and set the extension token
        # in a batch fashion
        expiry_cursor = self._get_db_cursor()
        expiry_cursor.execute(token_update_query)
        expiry_cursor.close()
        self.db_connection.commit()

        # Fetch the channel options from the river
        fetch_cursor = self._get_db_cursor()
        fetch_cursor.execute("""
            SELECT cf.river_id, cf.channel, cfo.`key`, cfo.`value`
            FROM channel_filters cf
            INNER JOIN channel_filter_options cfo
                ON (cfo.channel_filter_id = cf.id)
            WHERE cf.river_id IN (%s)
            AND cf.filter_enabled = 1
            """ % river_ids_str)

        # Pack each option in JSON format
        channel_options = []
        for river_id, channel, key, value in fetch_cursor.fetchall():
            option = {
                'channel': channel,
                'river_id': river_id,
                'key': key,
                'value': value}
            channel_options.append(option)

        fetch_cursor.close()

        # Deactivate the channels for the rivers
        disable_cursor = self._get_db_cursor()
        disable_cursor.execute("""
            UPDATE channel_filters SET filter_enabled = 0
            WHERE filter_enabled = 1
            AND river_id IN (%s)
            """ % river_ids_str)
        disable_cursor.close()
        self.db_connection.commit()

        # Get MQ connection and send channel delete commands
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.mq_host),
            pika.reconnection_strategies.SimpleReconnectionStrategy())

        channel = connection.channel()
        channel.exchange_declare(exchange="chatter",
                                 type="topic",
                                 durable=True)

        log.info("Purging channel options from the crawl schedules..")
        props = pika.BasicProperties(delivery_mode=2)
        for option in channel_options:
            # Generate the routing key
            routing_key = "web.channel_option.%s.delete" % option['channel']

            # Send the message to the MQ
            channel.basic_publish(exchange='chatter',
                                  routing_key=routing_key,
                                  body=json.dumps(option),
                                  properties=props,
                                  immediate=True)

    def _set_expiry_candidate_flag(self, river_ids):
        """Sets the expiry_candiate flag for the specified rivers"""
        river_ids_str = ', '.join(map(str, river_ids))
        cursor = self._get_db_cursor()
        cursor.execute("""
            UPDATE rivers SET expiry_candidate = 1
            WHERE `id` IN (%s)""" % river_ids_str)
        cursor.close()
        self.db_connection.commit()

    def _notify_owners(self, owners, data):
        """Sends emails to the owners of a river to notify them that
        their river has expired or has been marked for expiry"""

        active_duration = int(self.app_settings['river_active_duration'])
        for owner in owners:
            days_to_expiry = data['days_to_expiry']
            river_name = data['river_name']
            river_url = data['url']
            recipient = "%s <%s>" % (owner['name'], owner['email'])

            # Default subject and mail body value
            subject = "Your %s river will shutdown in %d days" %\
                (data['river_name'], days_to_expiry)

            mail_body = self.warning_template %\
                (owner['name'], river_name, days_to_expiry, days_to_expiry,
                 active_duration, river_url)

            # When the river has expired
            if days_to_expiry == 0:
                subject = "Your %s river has shutdown!" % data['river_name']

                # Mail body
                mail_body = self.notice_template %\
                    (owner['name'], river_name, active_duration, river_url)

            # Send the email
            p = popen("%s -t -i" % SENDMAIL, "w")
            p.write("From: %s\n" % self.email_from)
            p.write("To: %s\n" % recipient)
            p.write("Subject: %s\n\n" % subject)
            p.write(mail_body)
            status = p.close()
            if status != None:
                log.error("Error sending notification to %s" % recipient)


if __name__ == '__main__':
    # Load the configuration
    config = ConfigParser.SafeConfigParser()
    config.readfp(open(dirname(
        realpath(__file__)) + '/config/riverexpiry.cfg'))

    try:
        log_file = config.get("main", 'log_file')
        log_level = config.get("main", 'log_level')
        mq_host = config.get("main", 'mq_host')

        FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        log.basicConfig(filename=log_file,
                        level=getattr(log, log_level.upper()),
                        format=FORMAT)

        # Get the database config
        db_config = {
            'host': config.get("db", 'host'),
            'port': config.getint("db", 'port'),
            'user': config.get("db", 'user'),
            'pass': config.get("db", 'pass'),
            'database': config.get("db", 'database')}

        # Set the sendmail path
        SENDMAIL = config.get('email', 'sendmail')

        # Email configuration
        email_config = {
            'expiry_warning': config.get('email', 'expiry_warning'),
            'expiry_notice': config.get('email', 'expiry_notice'),
            'site_email': config.get('email', 'site_email')}

        # Instantiate expiry object
        expiry = RiverExpiry(mq_host, db_config, email_config)

        # Check for the issued command
        if len(sys.argv) == 2:
            if sys.argv[1] == 'start':
                # Start maintenance
                expiry.start()
            else:
                print "Uknown command"
                sys.exit(2)
        else:
            print "usage: %s start" % sys.argv[0]
            sys.exit(2)
    except ConfigParser.NoOptionError, e:
        log.error("Configuration error:  %s" % e)
