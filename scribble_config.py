#!/usr/bin/env python
#
# Created: 10:08 PM, December 9, 2011

import socket

class client:
    sleepTimeBetweenSends = 0.01

class server:
    intervalBetweenPolls = 0.01
    maxPollWait = 0.01
    host = '192.168.117.201'#socket.gethostbyname(socket.gethostname())
    port = 1985
    maxLogBufferSize = 1000
    flushWaitTime = 1
    maxConnectionBacklog = socket.SOMAXCONN

class cassandra:
    keyspace = "retickr_logs_for_testing"
    hosts = ["192.168.117.67"]#,"192.168.117.61","192.168.117.62","192.168.117.63"]
    server_port = 9160
    new_keyspace_strategy_options = {'replication_factor': '1'}
