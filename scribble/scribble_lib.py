#!/usr/bin/env python
"""

Scribble interface library

Scribble is our buffered write system for Cassandra.  To make it easier to
interface with, this library handles connecting to and talking to the server
scribble server.

@organization: retickr
@contact: josh.marlow+scribble@retickr.com
"""

import json
import socket
import time
import os
import sys


__conf_path__ = "/srv/scribble/scribble/scribble.conf"
__server_log_path__ = "/var/log/scribble/server.log"

def load_config_file():
    with open(os.path.join(__conf_path__)) as f:
        conf = json.loads(f.read())

    return conf


__conf__ = load_config_file()

__license__ = "Copyright (c) 2012, Retickr, LLC"
__organization__ = "Retickr, LLC"
__authors__ = [
    "Josh Marlow <josh.marlow+scribble@retickr.com>",
    "Micah Hausler <micah.hausler+scribble@retickr.com>",
    "Adam Haney <adam.haney+scribble@retickr.com"
    ]


class scribble_writer:

    def start_scribble_server(self):
        if 0 == os.fork():
            # Child
            os.setsid()
            os.umask(0)

            if 0 == os.fork():
                # Child but not a session leader
                # redirect standard file descriptors
                # TODO: redirect stdout to some log file for the server...
                sys.stdout.flush()
                sys.stderr.flush()

                try:
                    si = file('/dev/null', 'r')
                    so = file(__server_log_path__, 'a+')
                    se = file(__server_log_path__, 'a+', 0)
                except IOError, e:
                    print "IOError starting server: {0}".format(e)
                    sys.exit(0)

                os.dup2(si.fileno(), sys.stdin.fileno())
                os.dup2(so.fileno(), sys.stdout.fileno())
                os.dup2(se.fileno(), sys.stderr.fileno())

                # Time to become the server
                try:
                    server_name = "scribble_server.py"
                    os.execvp(server_name, [server_name])
                except OSError:
                    sys.exit(0)
            else:
                # Parent
                sys.exit(0)
        else:
            # Wait a couple of seconds for the server to get in full swing
            # then return to our life as a client
            sleepIncrement = 2
            time.sleep(sleepIncrement)

    def connect_to_server(self):
        def try_connect():
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((__conf__["server"]["host"],
                      int(__conf__["server"]["port"])))

            return s

        s = None

        for i in range(int(__conf__["client"]["maxClientConnectionAttempts"])):
            try:
                s = try_connect()
                break
            except socket.error:
                print "No scribble server found.  Startintg one..."
                self.start_scribble_server()
        else:
            print "Could not connect to server and could not start one." +\
                  "Giving up"
            sys.exit(0)

        return s

    def write_data(self, dataDictionary, superColumn=False):
        """
        Given a dictionary describing the write request to Cassandra, connect
        to the scribble server and send a message summarizing this write
        request.
        """

        neededKeys = [
            "keyspace",
            "columnFamily",
            "rowKey",
            "columnName",
            "value"
            ]

        if superColumn:
            neededKeys += ["superColumnName"]

        # Make sure all of the needed keys are in the dictionary
        for nKey in neededKeys:
            if nKey not in dataDictionary.keys():
                raise TypeError("Missing required key '{0}'".format(nKey))

        jsonData = json.dumps(dataDictionary)

        s = self.connect_to_server()

        if s:
            # Loop until we've sent all of the data
            while len(jsonData) > 0:
                bytesSent = s.send(jsonData)

                jsonData = jsonData[bytesSent:]

                time.sleep(float(__conf__["client"]["sleepTimeBetweenSends"]))

            s.shutdown(socket.SHUT_RDWR)
            s.close()
