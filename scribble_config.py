#!/usr/bin/env python
#
# Created: 10:08 PM, December 9, 2011

import socket

class client:
    sleepTimeBetweenSends = 0.01

class server:
    intervalBetweenPolls = 0.0005
    maxPollWait = 0.001
    host = '192.168.117.201'#socket.gethostbyname(socket.gethostname())
    port = 8080
    maxLogBufferSize = 1000
    flushWaitTime = 1
    maxConnectionBacklog = socket.SOMAXCONN

class cassandra:
    keyspace = "retickr_logs"
    hosts = ["192.168.117.64","192.168.117.61","192.168.117.62","192.168.117.63"]
    server_port=9160
