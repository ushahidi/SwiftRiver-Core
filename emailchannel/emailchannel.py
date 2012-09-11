#!/usr/bin/env python
# encoding: utf-8
"""
Email Channel

Implements an LMTP daemon that receives mail from an MTA, converts it into a
drop and delivers it to a river by publishing it to the droplet queue.


Copyright (c) 2012 Ushahidi. All rights reserved.
"""

import sys
import ConfigParser
import logging as log
import asyncore
import email
import hashlib
import dateutil.parser
from os.path import dirname, realpath
from smtpd import SMTPChannel, SMTPServer

import MySQLdb

from swiftriver import Daemon, DropPublisher
from cloudfiles.connection import ConnectionPool


class EmailDaemon(Daemon):

    def __init__(self,
                 port,
                 mq_host,
                 db_config,
                 pid_file,
                 out_file,
                 cf_options):
        Daemon.__init__(self, pid_file, out_file, out_file, out_file)

        self.port = port
        self.mq_host = mq_host
        self.db_config = db_config
        self.db = None
        self.cf_options = cf_options

    def run(self):
        log.info("Email Daemon Started")
        drop_publisher = DropPublisher(mq_host)
        server = LMTPServer(('localhost', self.port),
                            self.db_config,
                            drop_publisher,
                            self.cf_options)
        asyncore.loop()


class LMTPServer(SMTPServer):

    def __init__(self, localaddr, db_config, drop_publisher, cf_options):
        SMTPServer.__init__(self, localaddr, None)

        self.drop_publisher = drop_publisher
        self.db_config = db_config
        self.db = None
        self.cf_options = cf_options

    def process_message(self, peer, mailfrom, rcptto, data):
        try:
            self.__parse_message(peer, mailfrom, rcptto, data)
        except Exception, e:
            log.error("Error in processing %r" % e)
            return "451 Error in processing"

    def __parse_message(self, peer, mailfrom, rcptto, data):
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

        attachments = self.__get_attachments(msg)

        rivers = self.__get_rivers(rcptto)
        if not rivers:
            log.debug("No matching rivers")
            return

        if not msg.get("Subject"):
            log.debug("No subject")
            return

        droplet_date_pub = dateutil.parser.parse(msg.get("Date")) \
                                          .strftime('%Y-%m-%d %H:%M:%S')

        drop = {
            'channel': 'email',
            'river_id': rivers,
            'identity_orig_id': mailfrom,
            'identity_username': mailfrom,
            'identity_name': mailfrom,
            'identity_avatar': None,
            'droplet_orig_id': hashlib.md5((mailfrom + str(rcptto) + data)
                                           .encode('utf-8'))
                                           .hexdigest(),
            'droplet_type': 'original',
            'droplet_title': msg.get("Subject"),
            'droplet_content': content,
            'droplet_raw': content,
            'droplet_locale': None,
            'droplet_date_pub': droplet_date_pub}

        if attachments:
            drop['media'] = []
            for a in attachments:
                drop['media'].append({'url': a['url'],
                                      'type': a['type'],
                                      'droplet_image': False})

        self.drop_publisher.publish(drop)
        log.debug("Drop published to the following rivers: %r" % rivers)

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

    def __get_attachments(self, msg):
        """Push attachments to cloudfiles."""
        if not msg.is_multipart():
            return None

        urls = []
        cf_conn = self.cf_options['conn_pool'].get()
        container = cf_conn.get_container(self.cf_options['container'])
        for m in msg.get_payload()[1:]:
            payload = m.get_payload(decode=True)
            if m.get_content_maintype() == 'text':
                charset = m.get_content_charset('utf-8')
                payload = payload.decode(charset).encode('utf-8')
            filename = hashlib.sha256(payload).hexdigest()
            cloudfile = container.create_object(filename)
            cloudfile.content_type = m.get_content_type()
            cloudfile.write(payload)
            urls.append({'url': cloudfile.public_ssl_uri(),
                      'type': m.get_content_maintype()})
        self.cf_options['conn_pool'].put(cf_conn)
        return urls

    def __get_rivers(self, rcptto):
        c = self.get_cursor()
        format_strings = ','.join(['%s'] * len(rcptto))
        c.execute("""select `id`
                  from `rivers`
                  where email_id in (%s)""" % format_strings,
                  tuple([email.split('@')[0] for email in rcptto]))
        return [river_id for river_id, in c.fetchall()]

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
                  '/config/emailchannel.cfg'))

    try:
        log_file = config.get("main", 'log_file')
        out_file = config.get("main", 'out_file')
        pid_file = config.get("main", 'pid_file')
        log_level = config.get("main", 'log_level')
        listen_port = config.getint("main", 'listen_port')
        mq_host = config.get("main", 'mq_host')
        db_config = {
            'host': config.get("db", 'host'),
            'port': config.getint("db", 'port'),
            'user': config.get("db", 'user'),
            'pass': config.get("db", 'pass'),
            'database': config.get("db", 'database')}
        cf_options = {
            'username': config.get("cloudfiles", 'username'),
            'api_key': config.get("cloudfiles", 'api_key'),
            'container': config.get("cloudfiles", 'container')}
        cf_options['conn_pool'] = ConnectionPool(cf_options['username'],
                                                 cf_options['api_key'])

        FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        log.basicConfig(filename=log_file,
                        level=getattr(log, log_level.upper()), format=FORMAT)

        # Create outfile if it does not exist
        file(out_file, 'a')

        daemon = EmailDaemon(listen_port,
                             mq_host,
                             db_config,
                             pid_file,
                             out_file,
                             cf_options)
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
