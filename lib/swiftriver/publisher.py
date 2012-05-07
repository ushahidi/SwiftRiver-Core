"""
Producer threads that consume jobs from an in memory Queue and publish them to
the MQ.
"""

from threading import Thread
from Queue import Queue
import pika
import logging as log
import json


class Publisher(Thread):

    def __init__(self, name, mq_host, queue_name=None, exchange_name='',
                 exchange_type=None, routing_key='', durable=True):
        Thread.__init__(self)

        self.name = name
        self.mq_host = mq_host
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.routing_key = routing_key
        self.durable = durable
        self.q = Queue()
        self.start()
        log.info("%s started" % self.name)

    def publish(self, item):
        self.q.put(item, False)

    def run(self):
        connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self.mq_host),
                    pika.reconnection_strategies.SimpleReconnectionStrategy())
        channel = connection.channel()

        if self.queue_name is not None:
            channel.queue_declare(queue=self.queue_name, durable=self.durable)

        if self.exchange_name == '':
            self.routing_key = self.queue_name
        else:
            channel.exchange_declare(exchange=self.exchange_name,
                                     type=self.exchange_type,
                                     durable=self.durable)

        props = None
        if self.durable:
            props = pika.BasicProperties(delivery_mode=2)

        while True:
            item = self.q.get(True)
            channel.basic_publish(exchange=self.exchange_name,
                                  routing_key=self.routing_key,
                                  properties=props,
                                  body=json.dumps(item))


class DropPublisher(Publisher):

    DROP_QUEUE = 'DROPLET_QUEUE'

    def __init__(self, mq_host):
        Publisher.__init__(self, "Drop Publisher", mq_host,
                            queue_name='DROPLET_QUEUE')
