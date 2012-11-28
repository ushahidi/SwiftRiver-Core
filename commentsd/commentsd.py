#!/usr/bin/env python
# encoding: utf-8
"""
Email Channel

Implements an LMTP daemon that receives mail from an MTA, converts it into a
drop and delivers it to a river by publishing it to the droplet queue.


Copyright (c) 2012 Ushahidi. All rights reserved.
"""

import sys
import time
import ConfigParser
import logging as log
import asyncore
import email
import hashlib
import re
import json
from os.path import dirname, realpath
from smtpd import SMTPChannel, SMTPServer

import MySQLdb

from swiftriver import Daemon, DropPublisher
from cloudfiles.connection import ConnectionPool
from httplib2 import Http


class CommentsDaemon(Daemon):

    def __init__(self,
                 port,
                 db_config,
                 api_url,
                 api_key,
                 pid_file,
                 out_file):
        Daemon.__init__(self, pid_file, out_file, out_file, out_file)

        self.port = port
        self.db_config = db_config
        self.api_url = api_url
        self.api_key = api_key
        self.db = None

    def run(self):
        log.info("Comments Daemon Started")
        server = LMTPServer(('localhost', self.port),
                            self.db_config, self.api_url, self.api_key)
        asyncore.loop()


class LMTPServer(SMTPServer):

    def __init__(self, localaddr, db_config, api_url, api_key):
        SMTPServer.__init__(self, localaddr, None)

        self.db_config = db_config
        self.api_url = api_url
        self.api_key = api_key
        self.h = Http()
        self.db = None

    def process_message(self, peer, mailfrom, rcptto, data):
        try:
            self.__parse_message(peer, mailfrom, rcptto, data)
        except Exception, e:
            log.error("Error in processing %r" % e)
            return "451 Error in processing"

    def __parse_message(self, peer, mailfrom, rcptto, data):
        """Handle message data.

        Extract the message content and post it to the drop comment api endpoint

        """
        host, port = peer
        log.debug(("Received %d byte long message from %s:%d with " +
                   "envelope from '%s' to '%s'") %
                  (len(data),
                  host,
                  port,
                  mailfrom,
                  rcptto))

        msg = email.message_from_string(data)

        content = self.__get_content(msg)
        if not content:
            log.debug("No content found")
            return

        tokens = self.__get_tokens(rcptto)
        if not tokens:
            log.debug("No matching tokens")
            return
            
        comment = {
            'comment_text': content,
            'from_email': mailfrom}
            
        for token in tokens:
            token_type, data = token;
            log.debug("Posting %s comment for drop" % token_type,)
            
            resp = None
            if token_type == 'drop-comment':
                post_url = self.api_url + 'drop/' + str(data['drop_id']) + '/comment'
                post_url += '?api_key=' + api_key
                post_data = dict(comment.items() + 
                                {'context': data['context'], 
                                'context_obj_id': data['context_obj_id']}.items())
                resp, content = self.h.request(post_url, 'POST',
                                               body=json.dumps(post_data))
            else:
                post_url = self.api_url + 'bucket/' + str(data['bucket_id']) + '/comment'
                post_url += '?api_key=' + api_key
                resp, content = self.h.request(post_url, 'POST',
                                               body=json.dumps(comment))
            # If server error, keep retrying
            if resp.status >= 500:
                log.error("NOK response from the API (%d)" % resp.status)
                raise Exception

    def __get_content(self, msg):
        """Content is the first message part.

        For MIME messages, the first part is always text/plain.

        """
        if not msg.is_multipart():
            charset = msg.get_content_charset('utf-8')
            return msg.get_payload(decode=True) \
                        .decode(charset) \
                        .encode('utf-8') \
                        .replace('\n', '<br>\n') # Convert newlines to
                                                 # html line breaks
        else:
            return self.__get_content(msg.get_payload()[0])

    def __get_tokens(self, rcptto):
        c = self.get_cursor()
        format_strings = ','.join(['%s'] * len(rcptto))
        recipients = tuple([re.sub(r'^(drop|bucket)-comment-', r'', email.split('@')[0]) for email in rcptto])
        c.execute("""select `type`, `data`
                  from `auth_tokens`
                  where token in (%s)""" % format_strings, recipients)
        return [(token_type, json.loads(data)) for token_type, data in c.fetchall()]

    def handle_accept(self):
        conn, addr = self.accept()
        channel = LMTPChannel(self, conn, addr)

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
                    self.db.autocommit(True)

                self.db.ping(True)
                cursor = self.db.cursor()
            except MySQLdb.OperationalError:
                log.error(" error connecting to db, retrying")
                time.sleep(60)

        return cursor


class LMTPChannel(SMTPChannel):

    def smtp_LHLO(self, arg):
        """LMTP "LHLO" command is routed to the SMTP/ESMTP command"""
        self.smtp_HELO(arg)


if __name__ == "__main__":
    config = ConfigParser.SafeConfigParser()
    config.readfp(open(dirname(realpath(__file__)) +
                  '/config/commentsd.cfg'))

    try:
        log_file = config.get("main", 'log_file')
        out_file = config.get("main", 'out_file')
        pid_file = config.get("main", 'pid_file')
        log_level = config.get("main", 'log_level')
        listen_port = config.getint("main", 'listen_port')
        api_url = config.get("main", 'api_url')
        api_key = config.get("main", 'api_key')
        db_config = {
            'host': config.get("db", 'host'),
            'port': config.getint("db", 'port'),
            'user': config.get("db", 'user'),
            'pass': config.get("db", 'pass'),
            'database': config.get("db", 'database')}

        FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        log.basicConfig(filename=log_file,
                        level=getattr(log, log_level.upper()), format=FORMAT)

        # Create outfile if it does not exist
        file(out_file, 'a')

        daemon = CommentsDaemon(listen_port,
                             db_config,
                             api_url,
                             api_key,
                             pid_file,
                             out_file)
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
