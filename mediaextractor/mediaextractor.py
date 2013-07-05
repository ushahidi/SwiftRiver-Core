#!/usr/bin/env python
# encoding: utf-8
"""
Extracts links and images from droplets posted to the metadata fanout exchange
and publishes the update droplet back to the DROPLET_QUEUE for updating in
the db

Copyright (c) 2012 Ushahidi. All rights reserved.
"""


from threading import Thread
from os.path import dirname, realpath, basename
from urlparse import urlparse
import sys
import ConfigParser
import logging as log
import json
import re
import urllib2
import cStringIO
import hashlib
import urllib2
import socket
import ssl
import time

from PIL import Image
from httplib2 import Http
from httplib import BadStatusLine, InvalidURL
from cloudfiles.connection import ConnectionPool
import lxml.html

from swiftriver import Daemon, Consumer, Worker, Publisher


class MediaExtractorQueueWorker(Worker):

    def __init__(self, name, job_queue, confirm_queue, drop_publisher,
                 cf_options, url_services):
        self.drop_publisher = drop_publisher
        self.cf_options = cf_options
        self.url_services = url_services   
        Worker.__init__(self, name, job_queue, confirm_queue)

    def work(self):
        """Populate links and images in the drop and publish back to the
        droplet queue."""

        method, properties, body = self.job_queue.get(True)
        delivery_tag = method.delivery_tag
        start_time = time.time()
        droplet = json.loads(body)

        log.info(" %s drop received with correlation_id %s" %
                 (self.name, properties.correlation_id))

        links = []
        images = []
        
        if droplet['droplet_content']:
            # Get the urls from the anchor and image tags in the drop 
            media_tags = re.findall(
                '<\s*(a|img)\s*[^>]*(?:(?:href|src)\s*=\s*"([^"]+))"[^>]*>',
                droplet['droplet_raw'], re.I)
            
            for match in media_tags:
                tag, url = match
                if tag == 'img':
                    images.append(url)
                else:
                    url = self.get_full_url(url)
                    image, link = self.parse_link(url)
                    if image:
                        images.append(image)
                    elif link:
                        links.append(link)
            
            # Strip tags and get urls left in the html text
            url_items = re.findall("(?:https?://[^\\s]+)", re.sub(r'<[^>]*>', '',
                                   droplet['droplet_raw']).strip()) 
            
            for url in url_items:
                url = self.get_full_url(url)
                image, link = self.parse_link(url)
                if image:
                    images.append(image)
                elif link:
                    links.append(link)

            if links:
                if not droplet.has_key('links'):
                    droplet['links'] = links

            # Get a droplet_image and remove too small images
            droplet_image = None
            selected_images = []
            cur_max = 0
            for url in images:
                try:
                    f = cStringIO.StringIO(urllib2.urlopen(url.encode('utf-8')).read())
                    image = Image.open(f)
                    width, height = image.size
                    area = width * height
                    if area > 5000:
                        selection = {'url': url}
                        if area > cur_max:
                            cur_max = area
                            droplet_image = url
                        # Store thumbnail
                        if self.cf_options['enabled']:
                            image.thumbnail((200, 200), Image.ANTIALIAS)
                            thumbnail = cStringIO.StringIO()
                            filename = basename(urlparse(url)[2])
                            extension = filename[-3:].lower()
                            format = 'JPEG'
                            mime_type = 'image/jpeg'
                            if extension == 'png':
                                format = 'PNG'
                                mime_type = 'image/png'
                            elif extension == 'gif':
                                format = 'GIF'
                                mime_type = 'image/gif'
                            image.save(thumbnail, format)
                            cf_conn = self.cf_options['conn_pool'].get()
                            container = cf_conn.get_container(self.cf_options['container'])
                            cloudfile = container.create_object(hashlib.sha256(url.encode('utf-8')).hexdigest() +  '.' + extension)
                            cloudfile.content_type = mime_type
                            cloudfile.write(thumbnail.getvalue())
                            thumbnail_url = cloudfile.public_ssl_uri()
                            selection['thumbnails'] = [{'size': 200, 'url': thumbnail_url}]
                            self.cf_options['conn_pool'].put(cf_conn)
                        selected_images.append(selection)
                except IOError, e:
                    log.error(" %s IOError on image %s %r" % (self.name, url, e))
                except BadStatusLine, e:
                    log.error(" %s BadStatusLine on image %s %r" % (self.name, url, e))
                except ValueError, e:
                    log.error(" %s ValueError on image %s %r" % (self.name, url, e))
                except InvalidURL, e:
                    log.error(" %s InvalidURL on image %s %r" % (self.name, url, e))

            # Add selected images to drop                            
            if selected_images:
                droplet['media'] = []
                for image in selected_images:
                    media = {'url': image['url'],
                             'type': 'image',
                             'droplet_image': image['url'] == droplet_image}
                    if 'thumbnails' in image:
                        media['thumbnails'] = image['thumbnails']
                    droplet['media'].append(media)

        # Send back the updated droplet to the droplet queue for updating
        droplet['media_complete'] = True
        droplet['source'] = 'mediaextractor'
        
        # Some internal data for our callback
        droplet['_internal'] = {'delivery_tag': delivery_tag}
        
        self.drop_publisher.publish(droplet, 
                                    callback=self.confirm_drop, 
                                    corr_id=properties.correlation_id,
                                    routing_key=properties.reply_to)

        log.info(" %s finished processing in %fs" % (self.name, time.time()-start_time))
        
    def confirm_drop(self, drop):
        # Confirm delivery only once droplet has been passed
        # for metadata extraction
        self.confirm_queue.put(drop['_internal']['delivery_tag'], False)

    def parse_link(self, url):
        """Given a url, return the photo url from an image service or the url
        back."""

        image = None
        link = None
        if (re.search("\.(jpg|jpeg|png|gif)(?:[?#].*)?$", url, re.I)):
            image = url
        else:
            try:
                image_service_url = self.get_image_service_url(url)
                if image_service_url is not None:
                    image = image_service_url
                else:
                    link = {'url': url}
            except Exception, e:
                link = {'url': url}

        return image, link

    def get_image_service_url(self, url):
        ret = None

        domain = urlparse(url)[1]

        if domain == 'yfrog.com':
            ret = url.strip() + ':iphone'
        elif domain == 'plixi.com':
            dom = lxml.html.parse(url)
            photo = dom.find(".//img[@id='photo']")
            if photo is not None:
                ret = photo.get("src")
        elif domain == 'instagr.am':
            dom = lxml.html.parse(url)
            photo = dom.find(".//img[@class='photo']")
            if photo is not None:
                ret = photo.get("src")
        elif domain == 'twitpic.com':
            ret =  'http://twitpic.com/show/large/' + url.split('/')[3]
        elif domain == 'flic.kr':
            dom = lxml.html.parse(url)
            photo = dom.find(".//img[@class='photo']")
            if photo is not None:
                ret = photo.get("src")

        return ret

    def get_full_url(self, url, depth=1):
        """Make a raw HTTP HEAD request to get the full URL from a know 
        shortening service and shorten the result if it is also 
        shortened."""
        
        # Do a maximum of 5 recursions then give up
        if not self.is_short_url(url) or depth > 5:
            return url
        
        if not re.match("^https?://", url, re.I):
            url = "http://" + url
        status_line = None    
        try:
            url_parts = urlparse(url)
            port = 80
            if url_parts.port is not None:
                port = parts.port
            elif url_parts.scheme == "https":
                port = 443

            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if url_parts.scheme == "https":
                s = ssl.wrap_socket(s)
            s.connect((url_parts.hostname, port))
            s.send("HEAD %s HTTP/1.1\r\n" % url_parts.path)
            s.send("Host: %s\r\n" % url_parts.hostname)
            s.send("user-agent: Swiftriver/0.2 (gzip)\r\n")
            s.send("\r\n")
            f = cStringIO.StringIO(s.recv(4096))
            s.close()

            # First line is status line
            status_line = f.readline()
            
            if re.match('^HTTP/\d\.\d\s(1|2|3)\d\d\s', status_line, re.I):
                for line in f:
                    m = re.match("^Location:\s(.+)\r\n", line, re.I)
                    if m:
                        url = m.group(1)
            else:
                log.info(" %s bad status line %s" % (self.name, status_line))
            f.close()
            
            return self.get_full_url(url, depth+1)
        except Exception, e:
            log.error(" %s error expanding url %s %r" %
                      (self.name, url, e))

        return url
        
    def is_short_url(self, url):
        return urlparse(url)[1].lower().encode('utf-8') in self.url_services


class MediaExtractorQueueDaemon(Daemon):

    def __init__(self, num_workers, mq_host, cf_options, url_services, pid_file, out_file):
        Daemon.__init__(self, pid_file, out_file, out_file, out_file)

        self.num_workers = num_workers
        self.mq_host = mq_host
        self.cf_options = cf_options
        self.url_services = url_services

    def run(self):
        options = {'exchange_name': 'metadata', 
                   'exchange_type': 'fanout', 
                   'durable_queue': True,
                   'prefetch_count': self.num_workers}
        drop_consumer = Consumer("mediaextractor-consumer", self.mq_host, 
                                 'MEDIA_EXTRACTOR_QUEUE', options)        
        drop_publisher = Publisher("Response Publisher", mq_host)

        for x in range(self.num_workers):
            MediaExtractorQueueWorker("mediaextractor-worker-" + str(x), 
                                     drop_consumer.message_queue, 
                                     drop_consumer.confirm_queue, 
                                     drop_publisher, self.cf_options, 
                                     self.url_services)
        log.info("Workers started")

        drop_consumer.join()
        log.info("Exiting")


if __name__ == "__main__":
    config = ConfigParser.SafeConfigParser()
    config.readfp(open(
        dirname(realpath(__file__)) + '/config/mediaextractor.cfg'))

    try:
        log_file = config.get("main", 'log_file')
        out_file = config.get("main", 'out_file')
        pid_file = config.get("main", 'pid_file')
        num_workers = config.getint("main", 'num_workers')
        log_level = config.get("main", 'log_level')
        mq_host = config.get("main", 'mq_host')

        cf_options = {
            'enabled': config.getboolean("cloudfiles", 'enabled'),
            'username': config.get("cloudfiles", 'username'),
            'api_key': config.get("cloudfiles", 'api_key'),
            'container': config.get("cloudfiles", 'container')
        }
        if cf_options['enabled']:
            cf_options['conn_pool'] = ConnectionPool(cf_options['username'],
                                                     cf_options['api_key'])

        FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        log.basicConfig(filename=log_file,
                        level=getattr(log, log_level.upper()), format=FORMAT)

        file(out_file, 'a') # Create outfile if it does not exist
        
        # Load URL shortening services
        f = open(dirname(realpath(__file__)) + "/config/shorteners.dat")
        url_services = [line.strip() for line in f.readlines()]

        daemon = MediaExtractorQueueDaemon(num_workers, mq_host, cf_options, url_services,
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
