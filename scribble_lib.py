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

import scribble_config as conf


class scribble_writer:

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

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((conf.server.host, conf.server.port))

        # Loop until we've sent all of the data
        while len(jsonData) > 0:
            bytesSent = s.send(jsonData)

            jsonData = jsonData[bytesSent:]

            time.sleep(conf.client.sleepTimeBetweenSends)

        s.close()
