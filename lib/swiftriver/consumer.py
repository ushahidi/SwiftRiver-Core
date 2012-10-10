"""
Common infrastructure for the worker threads interacting with the MQ
"""

import logging as log
import socket
import time
import random
from threading import Thread
from Queue import Queue

import pika


class Consumer(Thread):
    """Base class for all the worker threads"""

    DROPLET_QUEUE = 'DROPLET_QUEUE'

    def __init__(self, name, mq_host, queue_name, options={}):
        Thread.__init__(self)

        self.name = name
        self.mq_host = mq_host
        self.queue_name = queue_name
        self.mq = None
        self.db = None
        self.message_queue = Queue()
        self.confirm_queue = Queue()
        self.pending = 0 # Number of unacknowledged messages from the MQ
        self.options = options

        # AMQP channel options
        self.exchange_name = options.get('exchange_name')
        self.exchange_type = options.get('exchange_type', 'fanout')
        self.routing_key = options.get('routing_key', '')
        self.durable_queue = options.get('durable_queue', False)
        self.durable_exchange = options.get('durable_exchange', True)
        self.prefetch_count = options.get('prefetch_count', 1)
        self.exclusive = options.get('exclusive', False)

        self.start()

    def run(self):
        """ Registers the handler that processes responses from the MQ"""

        log.info("Registering handler for %s" % self.name)
        # Connect to the MQ, retry on failure
        while True:
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self.mq_host),
                    pika.reconnection_strategies.SimpleReconnectionStrategy())

                channel = connection.channel()
                channel.queue_declare(queue=self.queue_name,
                                      durable=self.durable_queue,
                                      exclusive=self.exclusive)
                                      
                channel.basic_qos(prefetch_count=self.prefetch_count)

                if self.exchange_name:
                    channel.exchange_declare(exchange=self.exchange_name,
                                             type=self.exchange_type,
                                             durable=self.durable_exchange)
                    if isinstance(self.routing_key, basestring):
                        channel.queue_bind(exchange=self.exchange_name,
                                           queue=self.queue_name,
                                           routing_key=self.routing_key)
                    else:
                        # Bind channel to a list of routing keys
                        for rk in self.routing_key:
                            channel.queue_bind(exchange=self.exchange_name,
                                               queue=self.queue_name,
                                               routing_key=rk)

                channel.basic_consume(self.handle_message,
                                      queue=self.queue_name)
                channel.start_consuming()
            except socket.error, msg:
                log.error("%s error connecting to the MQ: %s. Retrying..." %
                          (self.name, msg))
                time.sleep(60 + random.randint(0, 120))
            except pika.exceptions.AMQPConnectionError, e:
                log.error("%s lost connection to the MQ, reconnecting" %
                          self.name)
                log.exception(e)
                time.sleep(60 + random.randint(0, 120))

    def handle_message(self, ch, method, properties, body):
        self.pending += 1
        self.message_queue.put((method, properties, body),
                               False)

        # Pika channels are neither thread safe nor can messages received on
        # one channel be confirmed in another. So we have to piggy back on the
        # basic_consume callback to confirm previously received messages that
        # were successfully batched and processed while taking care to ensure
        # we ack all received drops when the number of unacknowledged drops
        # reaches the prefetch count we sent to the MQ
        while (not self.confirm_queue.empty() or
               self.pending == self.prefetch_count):
            tag = self.confirm_queue.get(True)
            ch.basic_ack(delivery_tag=tag)
            self.pending -= 1
