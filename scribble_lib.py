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
import random

import scribble_config as conf


class scribble_writer:

    def connect_to_server(self):
        s = None

        for i in range(conf.client.maxClientConnectionAttempts):
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            except Exception:
                sleepIncrement = 0.1 + 2 * i**2 + random.random()
                time.sleep(sleepIncrement)

        if s:
            # We finally got a connection
            s.connect((conf.server.host, conf.server.port))
        else:
            print "Could not connect to server"

        return s


    def write_data(self, dataDictionary, superColumn=False):
        """Given a dictionary describing the write request to Cassandra, connect"""
        """to the scribble server and send a message summarizing this write"""
        """request."""
        neededKeys = ["keyspace", "columnFamily", "rowKey", "columnName", "value"]

        if superColumn:
            neededKeys += ["superColumnName"]

        # Make sure all of th eneeded keys are in the dictionary
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

                time.sleep(conf.client.sleepTimeBetweenSends)

            s.shutdown(socket.SHUT_RDWR)
            s.close()
