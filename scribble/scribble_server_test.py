#!/usr/bin/env python
# Created: 9:15 AM, December 12, 2011
# Note: the sever will *NOT* shutdown and report normally in unit tests,
# because the flushing method is being overridden

import unittest
import types

import scribble.scribble_server as scribble_server
import scribble.scribble_client as scribble_client
import scribble.scribble_lib as scribble_lib


class serverTest(unittest.TestCase):
    """Test out the scribble server"""

    def setUp(self):
        """Do basic set up of the server.  Override the push_to_flush"""
        """method on the server so that we can double check it's flushing"""
        """behavior"""
        self.server = scribble_server.scribble_server()

        # Override the buffer flushing mechanism so we can observ
        # the flushing behavior of the server
        self.bufferedOutput = dict()

        # Becuase 'self' will have a different meaning when
        # unitTestFlushLogBuffer becomes a method of the scribble_server,
        # we 'backup' the serverTest 'self' so it is still usable from
        # our overriden flush method. Thank you Mike Walters for showing me
        # this nifty trick.
        self_ = self

        def unit_test_push_to_flush_queue(self, logTuple):
            (ks, cf, data) = logTuple

            self_.bufferedOutput[cf] = self_.bufferedOutput.get(cf, {})
            self_.bufferedOutput[cf].update(data)

        self.server.push_to_flush_queue =\
                types.MethodType(unit_test_push_to_flush_queue, self.server)

    def tearDown(self):
        self.server.shutdown()
        self.server = None

        self.bufferedOutput = dict()

    def test_basic_buffering(self):
        """Test that basic buffer works"""

        baseLogMessage = "This is log record {0}"

        # Override the max buffer size so that we know when it should flush
        self.server.maxLogBufferSize = 5

        scribbleWriter = scribble_lib.scribble_writer()

        for idx in range(0, 4):
            scribble_client.write_to_server(scribbleWriter, baseLogMessage.format(idx),
                                            "Users")

            self.server.do_work()

        # There should be no buffered output yet
        self.assertEqual(0, len(self.bufferedOutput),
                            "Premature dumping of log buffer")

        scribble_client.write_to_server(scribbleWriter, baseLogMessage.format(4), "Users")

        # Manually 'run' the server for a bit
        [self.server.do_work() for i in range(15)]

        # Shutdown the server
        self.server.shutdown()

        rowCount = 0

        # Count the rows
        for colFam in self.bufferedOutput:
            for col in self.bufferedOutput[colFam].values():
                rowCount += len(col.values())

        # There should be buffered output now
        self.assertTrue(rowCount > 0,
                        "Buffered log data not dumped as expected")

        self.assertEqual(rowCount, 5,
                         "The expected amount of log data was not dumped")


if __name__ == "__main__":
    try:
        unittest.main()
    except KeyboardInterrupt, e:
        exit(0)
