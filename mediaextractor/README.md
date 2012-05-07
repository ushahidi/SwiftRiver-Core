SwiftCore Media Extractor
=========================

Droplet post processor that scans a droplet's content for links and images add adds them as metadata.

System Requirements
====================

 * Python 2.6+

Required Libraries
===================
The following extra Python libraries are required by the application:
 
  * Httplib2 (http://code.google.com/p/httplib2/)
  * lxml (http://lxml.de/)
  * PIL (http://www.pythonware.com/products/pil/)
  * cloudfiles (https://github.com/rackspace/python-cloudfiles)

Installation of Required Libraries
===================================

Ubuntu/Debian Installation
-------------------------------------

        $ sudo apt-get install python-pip python-pil python-lxml
        $ pip install cloudfiles
        
Running the applications
========================= 

  * Create and edit mediaextractor.cfg from the provided mediaextractor.cfg.template
  * To start/stop/restart:

        $ <INSTALL_PATH>/mediaextractor.py start/stop/restart