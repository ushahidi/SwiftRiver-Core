SwiftCore Email In Channel
==========================

LMTP Daemon that puts received mail onto the dropletqueue for matching rivers.

System Requirements
====================

 * Python 2.6+

Required Libraries
===================
The following extra Python libraries are required by the application:
 
  * MySQLdb (http://mysql-python.sourceforge.net/MySQLdb.html)
  * cloudfiles (https://github.com/rackspace/python-cloudfiles)
  * dateutil (http://labix.org/python-dateutil)

Installation of Required Libraries
===================================

Ubuntu/Debian Installation
-------------------------------------

        $ sudo apt-get install python-pip python-mysqldb
        $ sudo pip install python-cloudfiles
        
Running the applications
========================= 

  * Create and edit emailchannel.cfg from the provided emailchannel.cfg.template
  * To start/stop/restart:

        $ <INSTALL_PATH>/emailchannel.py start/stop/restart