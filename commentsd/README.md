SwiftCore Replies Daemon
==========================

LMTP Daemon that posts received mail as comments to drops.

System Requirements
====================

 * Python 2.6+

Required Libraries
===================
The following extra Python libraries are required by the application:
 
  * MySQLdb (http://mysql-python.sourceforge.net/MySQLdb.html)
  * Httplib2 (http://code.google.com/p/httplib2/)

Installation of Required Libraries
===================================

Ubuntu/Debian Installation
-------------------------------------

        $ sudo apt-get install python-mysqldb
        
Running the applications
========================= 

  * Create and edit emailchannel.cfg from the provided emailchannel.cfg.template
  * To start/stop/restart:

        $ <INSTALL_PATH>/commentsd.py start/stop/restart