SwiftCore Droplet Queue Processor
=================================

Adds droplets into SwiftRiver by posting droplets on the droplet queue to the /api/droplet
endpoint on the SwiftRiver web application.

System Requirements
====================

 * Python 2.6+

Required Libraries
===================
The following extra Python libraries are required by the application:
 
  * Httplib2 (http://code.google.com/p/httplib2/)
  * Pika (https://github.com/pika/pika) AMQP library for communicating with the MQ

Installation of Required Libraries
===================================

Ubuntu/Debian Installation
-------------------------------------

        $ sudo apt-get install python-pip python-httplib2 
        $ pip install pika
        
Running the applications
========================= 

  * Create and edit dropletqueue.cfg from the provided dropletqueue.cfg.template
  * To start/stop/restart:

        $ <INSTALL_PATH>/dropletqueue.py start/stop/restart
