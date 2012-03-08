"""
Common infrastructure for the worker threads interacting with the MQ

"""

import logging as log
import socket
import time
import pika
import MySQLdb
from threading import Thread

class Worker(Thread):
    """Base class for all the worker threads"""
    
    DROPLET_QUEUE = 'DROPLET_QUEUE'
    
    def __init__(self, name, mq_host, queue, options=None):
        Thread.__init__(self)
        self.name = name
        self.mq_host = mq_host
        self.queue = queue
        self.daemon = True
        self.mq = None
        self.db = None

        if options is None:
            options = {}
        
        self.options = options
        
        # AMQP channel options
        self.exchange_name = options.get('exchange_name')
        self.exchange_type = options.get('exchange_type', 'fanout')
        self.routing_key = options.get('routing_key', '')
        self.durable_queue = options.get('durable_queue', False)
        self.durable_exchange = options.get('durable_exchange', True)
           
    def run(self):
        """ Registers the handler that processes responses from the MQ"""
        
        log.info("Registering handler for %s" % self.name)
        
        # Connect to the MQ, retry on failure
        while True:
            try:
                self.mq = pika.BlockingConnection(
                                                  pika.ConnectionParameters(host=self.mq_host), 
                                                  pika.reconnection_strategies.SimpleReconnectionStrategy())
                
                worker_channel = self.mq.channel()
                worker_channel.queue_declare(queue=self.queue, durable=self.durable_queue)
                worker_channel.basic_qos(prefetch_count=1)
                
                # Check for exchange name
                if self.exchange_name:
                    
                    worker_channel.exchange_declare(exchange=self.exchange_name, 
                                                    type=self.exchange_type,
                                                    durable=self.durable_exchange)
                    
                    worker_channel.queue_bind(exchange=self.exchange_name, 
                                              queue=self.queue, 
                                              routing_key=self.routing_key)
                
                
                worker_channel.basic_consume(self.handle_mq_response, queue=self.queue)
                worker_channel.start_consuming()
            except socket.error, msg:
                log.error("%s error connecting to the MQ. Retrying..." % self.name)
            except pika.exceptions.AMQPConnectionError, e:
                log.error("%s lost connection to the MQ, reconnecting" % self.name)
                time.sleep(60)
  
    def get_cursor(self):
        """Get a db connection and attempt reconnection"""
        
        # TODO: Raise exception if DB config is missing
        
        db_config = self.options.get('db_config')
        cursor = None
        while not cursor:
            try:        
                if not self.db:
                    self.db = MySQLdb.connect(host=db_config['host'],
                                                port=db_config['port'],
                                                passwd=db_config['pass'], 
                                                user=db_config['user'],
                                                db=db_config['database'])
        
                self.db.ping(True)
                cursor = self.db.cursor()
            except MySQLdb.OperationalError:
                log.error(" error connecting to the database, retrying")
                time.sleep(60)
        
        return cursor;        
    
    def handle_mq_response(self, ch, method, properties, body):
        """Handles messages received from the queue"""
        pass