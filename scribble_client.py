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

import sys
import socket
import time
import uuid

import scribble_lib


def write_to_server(scribbleWriter, logMessage, columnFamily):
    """Log the logMessage to the server and store it under the specified"""
    """column family"""
    def build_row(connectionTime):
        """Generate a unique rowID for this particular log message"""
        return "{0}:{1}:{2}".format(connectionTime, socket.gethostname(),
                                    uuid.uuid1())

    connectionTime = str(time.time())

    dataDictionary = {'keyspace': scribble_lib.conf.cassandra.keyspace,
                       'columnFamily': columnFamily,
                       'rowKey': connectionTime,
                       'columnName': build_row(connectionTime),
                       'value': logMessage}

    scribbleWriter.write_data(dataDictionary)


def run(columnFamily):
    """Loop accepting input from stdin and writing it to the log server"""
    scribbleWriter = scribble_lib.scribble_writer()
    try:
        while True:
            logMessage = sys.stdin.readline().rstrip()
            if logMessage:
                write_to_server(scribbleWriter, logMessage, columnFamily)
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        columnFamily = sys.argv[1]
    else:
        sys.exit("You must supply a column family")

    run(columnFamily)
