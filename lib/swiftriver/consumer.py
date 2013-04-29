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
from pika.adapters import select_connection

select_connection.POLLER_TYPE = 'epoll'

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

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self.start()

    def run(self):
        log.info("Registering handler for %s" % self.name)

        # Connect to the MQ
        self._connection = self.connect()
        self._connection.ioloop.start()
    
    def connect(self):
        """ This method connects to the MQ and obtains the connection handle.
        When the connection is established, the on_connection_opened method
        will be invoked by pika

        """
        return pika.SelectConnection(
            pika.ConnectionParameters(host=self.mq_host),
            self.on_connection_opened,
            stop_ioloop_on_close=False)
        
    def on_connection_opened(self, connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It (pika) passes the handle to the connection
        object in case we need to use it

        """
        log.info("Connection opened")
        
        # Add the on_close_closecallback to the connection handle
        self._connection.add_on_close_callback(self.on_connection_closed)
        self.open_channel()
        
    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method will be invoked by pika when the connection to RabbitMQ
        is closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            log.info("Connection closed, reopening in 5 seconds: (%s) %s",
                reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        """Invoked by the IOLoop timer if the connection is closed. See the 
        on_connection_closed method
        
        """
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()
        if not self._closing:
            #Create a new connection
            self._connection = self.connect()
            self._connection.ioloop.start()
            
    
    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel
        """
        self._connection.close()
    
    def open_channel(self):
        """Opens a new channel and registers on_channel_opened to be invoked
        by pika when RabbitMQ responds that the channel is open

        """
        log.info("Channel opened")
        self._connection.channel(on_open_callback=self.on_channel_opened)
    
    def on_channel_opened(self, channel):
        """Invoked by pika when a channel is opened

        """
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)

        self._channel.queue_declare(self.on_queue_declare_ok,
            queue=self.queue_name, durable=self.durable_queue,
            exclusive=self.exclusive)

    def on_queue_declare_ok(self, method_frame):
        """Invoked by pika when the Queue.Declare RPC call made in 
        on_channel_opened has completed. In this method we will declare
        the exchange (if present) and bind the queue to it. When done,
        this method will set up the consumer

        """
        self._channel.basic_qos(prefetch_count=self.prefetch_count)

        if self.exchange_name:
            self._channel.exchange_declare(None, exchange=self.exchange_name,
                                     exchange_type=self.exchange_type,
                                     durable=self.durable_exchange)
            if isinstance(self.routing_key, basestring):
                self._channel.queue_bind(None, exchange=self.exchange_name,
                                   queue=self.queue_name,
                                   routing_key=self.routing_key)
            else:
                # Bind channel to a list of routing keys
                for rk in self.routing_key:
                    self._channel.queue_bind(None, exchange=self.exchange_name,
                                       queue=self.queue_name,
                                       routing_key=rk)

        # Set up the consumer
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self._consumer_tag = self._channel.basic_consume(self.handle_message,
            self.queue_name)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages

        """
        log.info("Consumer was cancelled remotely. Shutting down: %e",
                method_frame)
        if  self._channel:
            self._channel.close()

    def handle_message(self, channel, method, properties, body):
        self.pending += 1
        self.message_queue.put((method, properties, body), False)

        # Pika channels are neither thread safe nor can messages received on
        # one channel be confirmed in another. So we have to piggy back on the
        # basic_consume callback to confirm previously received messages that
        # were successfully batched and processed while taking care to ensure
        # we ack all received drops when the number of unacknowledged drops
        # reaches the prefetch count we sent to the MQ
        while (not self.confirm_queue.empty() or
               self.pending == self.prefetch_count):
            tag = self.confirm_queue.get(True)
            self._channel.basic_ack(delivery_tag=tag)
            self.pending -= 1
            log.info("Confirmed drop with delivery tags %s", tag)

    
    def on_cancel_ok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer

        """
        self._channel.close()

    def stop(self):
        """Performs a clean shutdown of the connection to RabbitMQ by stopping
        the consumer with RabbitMQ

        """
        logger.info("Stopping")
        self._closing = True
        if self._channel:
            self._channel.basic_cancel(self.on_cancel_ok, self._consumer_tag)
        self._connection.ioloop.stop()