#!/usr/bin/env python
#
# Created: 9:44 PM, December 9, 2011

import socket
import select
import time
import cPickle
import socket
import os
import uuid
import random
import pprint
import threading
import sys

import pycassa
from pycassa.system_manager import *
import thrift

import scribble_config as conf

class scribble_server:
    READ_FLAGS = select.POLLIN | select.POLLPRI
    CLOSE_FLAGS = select.POLLERR | select.POLLHUP | select.POLLNVAL

    def __init__(self):
        # Do misc configuration
        self.running = True

        self.clientCount = 0
        self.openClientCount = 0
        self.wentWrong = 0
        self.pushCount = 0
        self.popCount = 0

        self.host = conf.server.host
        self.port = conf.server.port
        self.intervalBetweenPolls = conf.server.intervalBetweenPolls
        self.maxPollWait = conf.server.maxPollWait
        self.maxLogBufferSize = conf.server.maxLogBufferSize

        self.keyspace 	= conf.cassandra.keyspace
        self.cassandra_host_list	= conf.cassandra.hosts
        self.cassandra_port	= conf.cassandra.server_port

        # Set up threads/queues and locks for the flushing process
        self.flushQueueLock = threading.Lock()
        self.flushQueue = list()

        self.spawnFlushThread()

        # set up the listening socket
        self.setupNetworking()

    def setupNetworking(self):
        try:
            self.listenSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error:
            print "Cannot create socket"
            exit(0)

        self.listenSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listenSocket.setblocking(0)

        self.listenSocket.bind((self.host, self.port))
        self.listenSocket.listen(conf.server.maxConnectionBacklog)

        self.listenFileNo = self.listenSocket.fileno()

        # Set up the fd to socket dictionary (this is for mapping Unix-style file descriptors to Python socket objects
        self.fdToClientTupleLookup = dict({self.listenFileNo : (self.listenSocket, '', '')})

        # Used to store the data read from each client
        self.clientLogData = dict()

        # set up the poller
        self.poller = select.poll()

        self.poller.register(self.listenSocket, scribble_server.READ_FLAGS)

        # Set up the log buffer
        self.logBuffer = dict()

    def addDataToBuffer(self, data, columnFamily, connectionTime):
        row = self.buildRow(connectionTime)

        # Make sure we have the needed entries in the dictionary
        self.logBuffer[columnFamily] = self.logBuffer.get(columnFamily, {connectionTime : {} })

        self.logBuffer[columnFamily][connectionTime] = self.logBuffer[columnFamily].get(connectionTime, {})

        self.logBuffer[columnFamily][connectionTime][row] = data

    def buildRow(self, connectionTime):
        return "{0}:{1}:{2}".format(connectionTime, socket.gethostname(), uuid.uuid1())

    def pushToFlushQueue(self, logTuple):
        self.pushCount += 1

        self.flushQueueLock.acquire()

        self.flushQueue += [logTuple]

        self.flushQueueLock.release()

    def popFromFlushQueue(self):
        self.flushQueueLock.acquire()

        if len(self.flushQueue) > 0:
            self.popCount += 1

            result = self.flushQueue[0]
            # Note: it is safe to use indexes out of bounds for slice notation
            self.flushQueue = self.flushQueue[1:]
        else:
            result = None

        self.flushQueueLock.release()

        return result

    def spawnFlushThread(self):
        self_ = self

        class flushThread(threading.Thread):
            def __init__(self):
                threading.Thread.__init__(self)

                self.running = True

                self.sysmgr = SystemManager(self_.cassandra_host_list[random.randint(0,len(self_.cassandra_host_list)-1)]+':'+str(self_.cassandra_port))

                self.cassandraPool = pycassa.ConnectionPool(keyspace = self_.keyspace,
                                              server_list = self_.cassandra_host_list)

            def run(self):
                # Grab something from the queue
                while self.running:
                    # Get a tuple to write
                    logTuple = self_.popFromFlushQueue()

                    if logTuple:
                        (columnFamily, columnDictionary) = logTuple

                        self.flushToCassandra(columnFamily, columnDictionary)

                    # Wait a bit
                    time.sleep(conf.server.flushWaitTime)

            def flushToCassandra(self, columnFamily, columnDictionary):
                print "Inserting into column family '{0}'".format(columnFamily)
                pprint.pprint(columnDictionary)
#                if columnFamily not in self.sysmgr.get_keyspace_column_families(self_.keyspace):
#                    self.sysmgr.create_column_family(
#                                    keyspace=self_.keyspace,
#                                    name=columnFamily,
#                                    comparator_type=UTF8_TYPE,
#                                    key_validation_class=UTF8_TYPE,
#                                    key_alias='data')

                # Connect to Cassandra and insert the data
#                cf = pycassa.ColumnFamily(self.cassandraPool, columnFamily)
#               Original code
#               row = str(int(time.time()))+':'+gethostname() +':'+ str(uuid.uuid1()) 
#               column = str(int(time.time()))
#
#               cf.insert(column,{row : line})
#               cf.insert('lastwrite', {'time' : column })
#                cf.batch_insert(columnDictionary)

            def shutdown(self):
                self.running = False

        # Start up the flush thread
        self.flushThread = flushThread()
        self.flushThread.start()

    def flushRemainder(self):
        columnFamilies = self.logBuffer.keys()
        for columnFamily in columnFamilies:
            self.pushToFlushQueue((columnFamily, self.logBuffer[columnFamily]))
            del self.logBuffer[columnFamily]

    def flushLogBuffer(self):
        columnFamilies = self.logBuffer.keys()
        for columnFamily in columnFamilies:
            # For each column family to log to
            rowCount = sum([len(self.logBuffer[columnFamily][col])
                            for col
                            in self.logBuffer[columnFamily]])

            print "cf: {0} row count: {1}, self running: {2}".format(columnFamily, rowCount, self.running)

            # It's like this, see?  If we are shutting down, forget the buffering
            # and flush it all to Cassandra!
            if (rowCount >= self.maxLogBufferSize) or not self.running:
                # Write to Cassandra
                try:
                    self.pushToFlushQueue((columnFamily, self.logBuffer[columnFamily]))
                except pycassa.NotFoundException:
                    pass
                except thrift.transport.TTransport.TTransportException:
                    pass
                except Exception, e:
                    print e 
                    pass

                del self.logBuffer[columnFamily]

    def handleEvent(self, res):
        fd, eventType = res

        if (fd == self.listenFileNo) and (eventType & scribble_server.READ_FLAGS):
            # New incomming connection
            client, clientAddress = self.listenSocket.accept()

            self.clientCount += 1
            self.openClientCount += 1

            client.setblocking(0)

            clientFd = client.fileno()

            self.fdToClientTupleLookup[clientFd] = (client, clientAddress, str(int(time.time())))
            self.clientLogData[clientFd] = ''
            self.poller.register(client, scribble_server.READ_FLAGS)
        else:
            # New data on an existing connection
            if eventType & scribble_server.READ_FLAGS:
                # We have data to read
                (client, address, connectionTime) = self.fdToClientTupleLookup[fd]
                data = client.recv(1024)

                if '' == data:
                    # Connection closed; clean up this socket and record the data
                    self.openClientCount -= 1

                    messageComponents = cPickle.loads(self.clientLogData[fd])

                    self.addDataToBuffer(messageComponents['log'], messageComponents['cf'], connectionTime)

                    self.poller.unregister(client)
                    client.close()
                    del self.clientLogData[fd]
                    del self.fdToClientTupleLookup[fd]
                else:
                    self.clientLogData[fd] += data
            elif eventType & scribble_server.CLOSE_FLAGS:
                # Something went wrong
                (client, address, connectionTime) = self.fdToClientTupleLookup[fd]

                self.poller.unregister(client)
                client.close()

                self.wentWrong += 1

    def doWork(self):
        """Do does any socket activity needed and then checks if the"""
        """buffered log needs to be dumped"""
        # Do any reading/processing that needs to be done
        for res in self.poller.poll(self.maxPollWait):
            try:
                self.handleEvent(res)
            except KeyboardInterrupt, e:
                raise e
            except Exception, e:
                print e

        # Dump the log data if needed
        self.flushLogBuffer()

    def run(self):
        try:
            while self.running:
                self.doWork()
                time.sleep(self.intervalBetweenPolls)
        except KeyboardInterrupt:
            return

    def shutdown(self):
        self.runing = False

        # Flush anything that's left...
        self.flushRemainder()

        # Give the flush thread time to try to clear things out...
        time.sleep(3)

        [self.poller.unregister(client) for client in
            [clientTuple[0] for clientTuple in self.fdToClientTupleLookup.values()]]

        self.flushThread.shutdown()
        self.flushThread.join()
        sys.stdout.flush()

        self.report()

    def report(self):
        print "Shutting down"
        print "\tServed clients: {0}".format(self.clientCount)
        print "\tStill connected: {0}".format(self.openClientCount)
        print "\tWent wrong: {0}".format(self.wentWrong)
        print "\tColumn families not flushed: {0}".format(len(self.flushQueue))
        print "\tTotal pushes to queue: {0}".format(self.pushCount)
        print "\tTotal pops from queue: {0}".format(self.popCount)


if __name__ == "__main__":
    srv = scribble_server()
    srv.run()
    srv.shutdown()
