SwiftCore Twitter Streaming
===========================
Gets content from Twitter, using track and filter predicates, via the streaming API.

__NOTE__: Tweepy doesn't handle UTF-8 predicates very well and this causes a `UnicodeEncodeError` 
exception to be thrown. You'll therefore need to apply this patch - https://github.com/tweepy/tweepy/pull/142 - to `streaming.py`
Alternatively, you can grab a copy of `streaming.py` that already includes this patch from https://gist.github.com/165ff4cb6664dcc5942c

System Requirements
====================

 * Python 2.6+

Required Libraries
===================
The following extra Python libraries are required by the application:
 
  * Httplib2 (http://code.google.com/p/httplib2/)
  * Pika (https://github.com/pika/pika) AMQP library for communicating with the MQ
  * MySQLdb (http://mysql-python.sourceforge.net/MySQLdb.html)
  * Tweepy (http://github.com/tweepy/tweepy) for communicating with the Twitter Streaming API

Installation of Required Libraries
===================================

Ubuntu/Debian Installation
-------------------------------------

        $ sudo apt-get install python-pip python-mysqldb python-httplib2
        $ pip install pika
        $ pip install tweepy
        ˝
Running the applications
========================= 

  * Create and edit firehose.cfg and manager.cfg from their respective .template files
  * To start/stop/restart the Firehose manager:

		$ <INSTALL_PATH>/manager.py start/stop/restart

  * To start/stop/restart the firehose:
  
        $ <INSTALL_PATH>/firehose.py start/stop/restart