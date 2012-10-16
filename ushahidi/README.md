# Ushahidi Core Application
This application pushes drops to an Ushahidi deployment. It receives the "web.bucket.push.ushahidi"
messages sent to the MQ and:

 * Decodes the message (from JSON into a dict object)
 * Encodes the drops payload - first to JSON then to base64
 * Computes a SHA256 hmac of the encoded payload. This checksum is recomputed when the drops are posted to Ushahidi to verify that the data was not modified during transit
 * Submits the drops to the provided `post_url` via HTTP POST

__NOTE__ The payload from the MQ contains the following information:

 * Drops to be posted
 * ID of the bucket that the drops belong to
 * Credentials for the target deployment
 * URL of the target deployment

## System Requirements

 * Python 2.6+

## Required Libraries
The following extra Python libraries are required by the application:
 
  * Httplib2 (http://code.google.com/p/httplib2/)

## Ubuntu/Debian Installation

	$ sudo apt-get install python-pip python-imaging python-lxml
	$ pip install python-cloudfiles

## Running the applications

  * Create and edit ushahidi.cfg from the provided ushahidi.cfg.template
  * To start/stop/restart:

        $ <INSTALL_PATH>/ushahidi.py start/stop/restart