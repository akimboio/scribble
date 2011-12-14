#!/usr/bin/env python
# Created: 9:15 AM, December 12, 2011

import unittest
import socket
import time
import types

import scribble_config as conf
import scribble_server
import cPickle

class serverTest(unittest.TestCase):
    def logToServer(self, logMessage):
        """Establish a connection to the server and write this message"""

        # Connect to the server
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.server.host, self.server.port))

        data = logMessage

        while len(data) > 0:
            bytesSent = s.send(data)

            data = data[bytesSent:]

            time.sleep(conf.client.sleepTimeBetweenSends)

        s.close()

    def setUp(self):
        self.server = scribble_server.scribble_server()

        # Override the buffer flushing mechanism so we can observ
        # the flushing behavior of the server
        self.bufferedOutput = []

        # Becuase 'self' will have a different meaning when
        # unitTestFlushLogBuffer becomes a method of the scribble_server,
        # we 'backup' the serverTest 'self' so it is still usable from
        # our overriden flush method. Thank you Mike Walters for showing me
        # this nifty trick.
        self_ = self

        def unitTestPushToFlushQueue(self, logTuple):
            (cf, data) = logTuple
            self_.bufferedOutput = data

        self.server.pushToFlushQueue = types.MethodType(unitTestPushToFlushQueue, self.server)

    def tearDown(self):
        self.server.shutdown()
        self.server = None

        self.bufferedOutput = []

    def test_basic_buffering(self):
        """Test that basic buffer works"""

        baseLogMessage = "This is log record {0}"

        # Override the max buffer size so that we know when it should flush
        self.server.maxLogBufferSize = 5

        for idx in range(0, 4):
            data = cPickle.dumps({"log": baseLogMessage.format(idx),
                                  "cf": "Users "})
            self.logToServer(data)

            self.server.doWork()

        # There should be no buffered output yet
        self.assertEqual(0, len(self.bufferedOutput), "Premature dumping of log buffer")

        data = cPickle.dumps({"log": baseLogMessage.format(4),
                              "cf": "Users "})
        self.logToServer(data)

        # Manually 'run' the server for a bit
        [self.server.doWork() for i in range(15)]

        rowCount = sum([len(self.bufferedOutput[col]) for col in self.bufferedOutput])

        # There should be buffered output now
        self.assertTrue(rowCount > 0, "Buffered log data not dumped as expected")

        self.assertEqual(rowCount, 5, "The expected amount of log data was not dumped")


if __name__ == "__main__":
    unittest.main()
