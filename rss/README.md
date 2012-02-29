SwiftCore RSS
=============
RSS content fetch scheduler and workers.

System Requirements
====================

 * Python 2.6+

Required Libraries
===================
The following extra Python libraries are required by the application:
 
  * Universal Feed Parser (http://code.google.com/p/feedparser/)
  * MySQLdb (http://mysql-python.sourceforge.net/MySQLdb.html)
  * Pika (https://github.com/pika/pika) AMQP library for communicating with the MQ

Installation of Required Libraries
===================================

Ubuntu/Debian Installation
-------------------------------------

        $ sudo apt-get install python-mysqldb python-pip
        $ pip install pika
        $ pip install feedparser
        
Running the applications
========================= 

  * Create the rss_url and rss_cache tables from the provided install/rss.sql script
  * Create and edit rss-fetcher.cfg and rss-scheduler.cfg from their .template files
  * To start/stop/restart the scheduler:

        $ <INSTALL_PATH>/rss-scheduler.py start/stop/restart

  * To start/stop/restart the fetchers

        $ <INSTALL_PATH>/rss-fetcher.py start/stop/restart