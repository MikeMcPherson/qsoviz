# qsoviz
Compute and display interesting operational statics for ARRL Field Day or similar contest.  In its current version, qsoviz requires the following infrastructure:

* A server running some flavor of Linux.  I use Ubuntu, but any should work.  I use an off-site server, so we get both visualization and backup of our logs at the same time.
* Running on that Linux server: a working Python 3 environment, MySQL (or compatible, such as MariaDB), InfluxDB, Grafana.
* N3FJP's ARRL Field Day Contest Log logging software at your Field Day site.
* A large display at your Field Day site.  I use a Google Chromecast to drive the display, but any device with a browser (or the built-in browser on the display) should work.  

The flow is as follows:

* On the logging server at your site, uploadacfdlog.py (scheduled to run periodically) uploads to the Linux server either the raw N3FJP FD Log database or the backup ADIF file that FD Log can generate.  I use the built-in Windows scheduler to run this script on a schedule.
* On the Linux server, qsomysql.py reads either the FD Log database or the ADIF file and puts the results in a MySQL database.  I use incron to trigger execution of the script on the log database upload, but you could also use cron to run the script on a schedule.
* On the Linux server, qsoviz.py reads the MySQL database, generates statistics (such as contacts per hour), and writes the results to InfluxDB for consumption by Grafana.  Execution of this script should be triggered by completion of qsomysql.py or with cron.
* On the Linux server, Grafana reads the georeferenced contact data and statistics from InfluxDB and creates the display, which can be viewed in any Web browser.
* At your FD site, a Web browser connected to the large display shows the Grafana visualization.
