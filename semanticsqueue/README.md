SwiftCore Semantic Queue Processor
===================================

Posts new droplets to the Semantics API and places the droplets updated with tags back onto
the droplet queue for updating.

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

        $ sudo apt-get install python-pip
        $ pip install pika
        
Running the applications
========================= 

  * Create and edit linkextractor.cfg from the provided linkextractor.cfg.template
  * To start/stop/restart:

        $ <INSTALL_PATH>/linkextractor.py start/stop/restart