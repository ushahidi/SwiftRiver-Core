SwiftCore Maintenance Apps
==========================
A collection of applications for carrying out routine maintenance work
on the rivers. Maintenance is comprised of the following applications:

 * riverexpiry.py - Checks for the rivers that are due for expiry and sends out
   email notifications to their owners


System Requirements
===================

 * Python 2.6+
 * sendmail

Installation of Required Libraries
==================================

Debian/Ubuntu Installation
--------------------------

		$ sudo apt-get install sendmail

Running the Applications
========================
These applications are designed to run as cron jobs. Add the following
lines to your crontab to schedule the river expiry script to be
run everyday at midnight

		* 0 * * * * cd <app home>; python riverexpiry.py start >> logs/riverexpiry.log 2>&1