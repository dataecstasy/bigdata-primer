#------------------------------------------#
#Web Server Log Analysis with Apache Spark #
#------------------------------------------#

This assignment consists of 4 parts:
Part 1: Apache Web Server Log file format.
Part 2: Data Pre processing --> Converting structured format to unstructured format.
Part 3: Sample Log Analysis.
Part 4: Solving exercise provided.

#------------------------------------------#
# 	understanding log format 	   #
#------------------------------------------#

The log files that we use for this assignment are in the Apache Common Log Format (CLF). 
The log file entries produced in CLF will look something like this:

127.0.0.1 - - [01/Aug/1995:00:00:01 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1839

Each part of this log entry is described below.

    127.0.0.1 
	 ####This is the IP address (or host name, if available) of the client (remote host) which made the request to the server.

	 ####The "hyphen" in the output indicates that the requested piece of information (user identity from remote machine) is not available.
		
	 ####The "hyphen" in the output indicates that the requested piece of information (user identity from local logon) is not available.

    [01/Aug/1995:00:00:01 -0400] 
	####The time that the server finished processing the request. The format is: [day/month/year:hour:minute:second timezone]
        ####day = 2 digits
        ####month = 3 letters
        ####year = 4 digits
        ####hour = 2 digits
        ####minute = 2 digits
        ####second = 2 digits
        ####zone = (+ | -) 4 digits

    "GET /images/launch-logo.gif HTTP/1.0" 
	####This is the first line of the request string from the client. It consists of a three components: the request method (e.g., GET, POST, etc.), the endpoint 		(a Uniform Resource Identifier), and the client protocol version.

    200 
	####This is the status code that the server sends back to the client. This information is very valuable, because it reveals whether the request resulted in a 		successful response (codes beginning in 2), a redirection (codes beginning in 3), an error caused by the client (codes beginning in 4), or an error in the 		server 	(codes beginning in 5). The full list of possible status codes can be found in the HTTP specification (RFC 2616 section 10).

    1839 
	####The last entry indicates the size of the object returned to the client, not including the response headers. If no content was returned to the client, 		this value will be "-" (or sometimes 0).


#---------------------------------------------------------#
#          Data Preprocessing in Spark using Python       #
#---------------------------------------------------------#

import re
import datetime

from pyspark.sql import Row

month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
    'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}

def parse_apache_time(s):
    """ Convert Apache time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format
    Returns:
        datetime: datetime object (ignore timezone for now)
    """
    return datetime.datetime(int(s[7:11]),
                             month_map[s[3:6]],
                             int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))


def parseApacheLogLine(logline):
    """ Parse a line in the Apache Common Log format
    Args:
        logline (str): a line of text in the Apache Common Log format
    Returns:
        tuple: either a dictionary containing the parts of the Apache Access Log and 1,
               or the original invalid log line and 0
    """
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return (logline, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = long(0)
    else:
        size = long(match.group(9))
    return (Row(
        host          = match.group(1),
        client_identd = match.group(2),
        user_id       = match.group(3),
        date_time     = parse_apache_time(match.group(4)),
        method        = match.group(5),
        endpoint      = match.group(6),
        protocol      = match.group(7),
        response_code = int(match.group(8)),
        content_size  = size
    ), 1)

# A regular expression pattern to extract fields from the log line
APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'

#------------------------------------------------------------------------#
#                Initial RDD Creation -- Spark Abstraction               #
#------------------------------------------------------------------------#


import sys
import os

logFile='Mention the path of the file'

def parseLogs():
    """ Read and parse log file """
    parsed_logs = (sc
                   .textFile(logFile)
                   .map(parseApacheLogLine)
                   .cache())
    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache())
    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0]))
    failed_logs_count = failed_logs.count()
    if failed_logs_count > 0:
        print 'Number of invalid logline: %d' % failed_logs.count()
        for line in failed_logs.take(20):
            print 'Invalid logline: %s' % line
    print 'Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (parsed_logs.count(), access_logs.count(), failed_logs.count())
    return parsed_logs, access_logs, failed_logs


# Change in regular expression:

APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)'
parsed_logs, access_logs, failed_logs = parseLogs()


#----------------------------------------------------------------------#
#                      Sample Log Anaysis                              #
#----------------------------------------------------------------------#

 ######## Content size statistics ############

content_sizes = access_logs.map(lambda log: log.content_size).cache()
print 'Content Size Avg: %i, Min: %i, Max: %s' % (
    content_sizes.reduce(lambda a, b : a + b) / content_sizes.count(),
    content_sizes.min(),
    content_sizes.max())


 ####### Response Code Analysis ##############

responseCodeToCount = (access_logs
                       .map(lambda log: (log.response_code, 1))
                       .reduceByKey(lambda a, b : a + b))
responseCodeToCountList = responseCodeToCount.take(100)
print 'Found %d response codes' % len(responseCodeToCountList)
print 'Response Code Counts: %s' % responseCodeToCountList

 ####### Top Ten Error Endpoints ##############

from operator import add

not200 = access_logs.filter(lambda log: log.response_code != 200)

endpointCountPairTuple = not200.map(lambda log: (log.endpoint, 1))

endpointSum = endpointCountPairTuple.reduceByKey(add)

topTenErrURLs = endpointSum.takeOrdered(10, lambda s: -1 * s[1])


 ####### Find response code distribution for each day? #########
 ####### Composite Key value pairs                     #########

responseCodeToCount = access_logs.map(lambda log: ((log.response_code, log.date_time.day),1))
                       .reduceByKey(lambda a, b : a + b))

responseCodeToCount.saveAsTextFile("Path of the file)


#----------------------------------------------------------------------#
# Assignment for this week: Solve the tasks in exercises               #
#----------------------------------------------------------------------#
