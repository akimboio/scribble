#! /usr/bin/python
"""
scribble.py - python logging utility to cassandra

@organization: retickr
@contact: josh.marlow+scribble@retickr.com

usage:
        <STDIN> | scribble.py <column family>
            or in httpd.conf
        CustomLog "|| /path/to/scribble.py <column family>" combined
            Double pipe is necessisary to not kill your cpu
            See http://httpd.apache.org/docs/current/logs.html#piped

        where
            <STDIN> is a process, ex: "tail -f", apache log
            <column family> is the name of the column family,
                probably corresponding to vhost
"""

import cPickle
import sys
import socket
import time

import scribble_config as conf


def write_to_server(logMessage, columnFamily):
    data = cPickle.dumps({'log': logMessage, 'cf': columnFamily})

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((conf.server.host, conf.server.port))

        # Loop until we've sent all of the data
        while len(data) > 0:
            bytesSent = s.send(data)

            data = data[bytesSent:]

            time.sleep(conf.client.sleepTimeBetweenSends)

        s.close()
    except:
        pass


def run(columnFamily):
    try:
        while True:
            logMessage = sys.stdin.readline().rstrip()
            if logMessage:
                write_to_server(logMessage, columnFamily)
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        columnFamily = sys.argv[1]
    else:
        sys.exit("You must supply a column family")

    print "Using column family '{0}'".format(columnFamily)

    run(columnFamily)
