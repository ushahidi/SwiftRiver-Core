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

from PIL import Image
from httplib2 import Http
from cloudfiles.connection import ConnectionPool
import lxml.html

from swiftriver import Daemon, Consumer, Worker, DropPublisher


class MediaExtractorQueueWorker(Worker):

    def __init__(self, name, job_queue, confirm_queue, drop_publisher,
                 cf_options):
        self.drop_publisher = drop_publisher
        self.cf_options = cf_options
        Worker.__init__(self, name, job_queue, confirm_queue)

    def work(self):
        """Populate links and images in the drop and publish back to the
        droplet queue."""

        routing_key, delivery_tag, body = self.job_queue.get(True)
        droplet = json.loads(body)            
        log.info(" %s droplet received with id %d" %
                 (self.name, droplet.get('id', 0)))

        links = []
        images = []

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
                        cloudfile = container.create_object(hashlib.md5(url.encode('utf-8')).hexdigest() + '_' + filename)
                        cloudfile.content_type = mime_type
                        cloudfile.write(thumbnail.getvalue())
                        thumbnail_url = cloudfile.public_ssl_uri()
                        selection['thumbnails'] = [{'size': 200, 'url': thumbnail_url}]
                        self.cf_options['conn_pool'].put(cf_conn)
                    selected_images.append(selection)
            except IOError, e:
                pass

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

        self.drop_publisher.publish(droplet)

        # Confirm delivery only once droplet has been passed
        # for metadata extraction
        self.confirm_queue.put(delivery_tag, False)
        log.info(" %s finished processing" % (self.name,))

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

    def get_full_url(self, url):
        """Get the full URL but only do so if the link looks like a shortened
        url."""
        
        try:
            domain = urlparse(url)[1]
            
            if len(url) < 25 and len(domain) < 10:
                request = urllib2.Request(url)
                request.get_method = lambda : 'HEAD'
                response = urllib2.urlopen(request)
                url = response.geturl()
        except Exception, e:
            log.error(" %s error expanding url %s %r" %
                      (self.name, url, e))

        return url


class MediaExtractorQueueDaemon(Daemon):

    def __init__(self, num_workers, mq_host, cf_options, pid_file, out_file):
        Daemon.__init__(self, pid_file, out_file, out_file, out_file)

        self.num_workers = num_workers
        self.mq_host = mq_host
        self.cf_options = cf_options

    def run(self):
        options = {'exchange_name': 'metadata', 
                   'exchange_type': 'fanout', 
                   'durable_queue': True,
                   'prefetch_count': self.num_workers}
        drop_consumer = Consumer("mediaextractor-consumer", self.mq_host, 
                                 'MEDIA_EXTRACTOR_QUEUE', options)        
        drop_publisher = DropPublisher(mq_host)

        for x in range(self.num_workers):
            MediaExtractorQueueWorker("mediaextractor-worker-" + str(x), 
                                     drop_consumer.message_queue, 
                                     drop_consumer.confirm_queue, 
                                     drop_publisher, self.cf_options)
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

        daemon = MediaExtractorQueueDaemon(num_workers, mq_host, cf_options,
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
