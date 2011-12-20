#!/usr/bin/env python
#
# Created: 9:44 PM, December 9, 2011

import socket
import select
import time
import cPickle
import uuid
import random
import pprint
import threading
import sys
import signal
import Queue

import pycassa
import thrift

import scribble_config as conf


def pstderr(msg):
    sys.stderr.write("\n" + msg + "\n")


class scribble_server:
    POLL_READ_FLAGS = select.POLLIN | select.POLLPRI
    POLL_CLOSE_FLAGS = select.POLLERR | select.POLLHUP | select.POLLNVAL

    EPOLL_READ_FLAGS = select.EPOLLIN | select.EPOLLPRI
    EPOLL_CLOSE_FLAGS = select.EPOLLERR | select.EPOLLHUP

    READ_FLAGS = POLL_READ_FLAGS
    CLOSE_FLAGS = POLL_CLOSE_FLAGS

    def __init__(self, useEpoll=False,
                 intervalBetweenPolls=conf.server.intervalBetweenPolls,
                 maxPollWait=conf.server.maxPollWait):
        # Do misc configuration
        self.running = True
        self.shutdownComplete = False
        self.shuttingdown = False

        # Status
        self.clientCount = 0
        self.openClientCount = 0
        self.wentWrong = 0
        self.pushCount = 0
        self.popCount = 0
        self.rowsFlushed = 0

        self.useEpoll = useEpoll
        self.host = conf.server.host
        self.port = conf.server.port
        self.intervalBetweenPolls = intervalBetweenPolls
        self.maxPollWait = maxPollWait
        self.maxLogBufferSize = conf.server.maxLogBufferSize

        self.keyspace = conf.cassandra.keyspace
        self.cassandra_host_list = conf.cassandra.hosts
        self.cassandra_port = conf.cassandra.server_port

        self.flushQueue = Queue.Queue()

        self.spawn_flush_thread()

        # set up the listening socket
        self.setup_networking()

    def setup_networking(self):
        # Set up the fd to socket dictionary (this is for mapping Unix-style
        # file descriptors to Python socket objects
        self.fdToClientTupleLookup = dict()

        # Used to store the data read from each client
        self.clientLogData = dict()

        # Set up the log buffer
        self.logBuffer = dict()

        self_ = self

        # set up the poller
        if self.useEpoll:
            self.poller = select.epoll()
            scribble_server.READ_FLAGS = scribble_server.EPOLL_READ_FLAGS
            scribble_server.CLOSE_FLAGS = scribble_server.EPOLL_CLOSE_FLAGS
        else:
            self.poller = select.poll()
            scribble_server.READ_FLAGS = scribble_server.POLL_READ_FLAGS
            scribble_server.CLOSE_FLAGS = scribble_server.POLL_CLOSE_FLAGS

        # set up the thread to accept connections
        class acceptConnectionThread(threading.Thread):
            def __init__(self):
                threading.Thread.__init__(self)
                self.running = True

                try:
                    self.listenSocket = socket.socket(socket.AF_INET,
                                                      socket.SOCK_STREAM)
                except socket.error:
                    print "Cannot create socket"
                    sys.exit(0)

                self.listenSocket.setsockopt(socket.SOL_SOCKET,
                                             socket.SO_REUSEADDR, 1)
                self.listenSocket.setblocking(1)

                self.listenSocket.bind((self_.host, self_.port))
                self.listenSocket.listen(conf.server.maxConnectionBacklog)

                self.listenFileNo = self.listenSocket.fileno()

            def run(self):
                try:
                    while self.running:
                        client, clientAddress = self.listenSocket.accept()

                        self_.clientCount += 1
                        self_.openClientCount += 1

                        client.setblocking(0)

                        clientFd = client.fileno()

                        self_.fdToClientTupleLookup[clientFd] =\
                                (client, clientAddress, str(int(time.time())))
                        self_.clientLogData[clientFd] = ''
                        self_.poller.register(client,
                                              scribble_server.READ_FLAGS)
                except Exception, e:
                    print "Exception", e, type(e).__name__

            def shutdown(self):
                self.running = False

                self.listenSocket.close()

                # Connect so that accept will return...
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((self_.host, self_.port))
                s.close()

        self.acceptThread = acceptConnectionThread()
        self.acceptThread.start()

    def add_data_to_buffer(self, data, columnFamily, connectionTime):
        row = self.build_row(connectionTime)

        # Make sure we have the needed entries in the dictionary
        self.logBuffer[columnFamily] =\
                self.logBuffer.get(columnFamily, {connectionTime: {}})

        self.logBuffer[columnFamily][connectionTime] =\
                self.logBuffer[columnFamily].get(connectionTime, {})

        self.logBuffer[columnFamily][connectionTime][row] = data

    def build_row(self, connectionTime):
        return "{0}:{1}:{2}".format(connectionTime, socket.gethostname(),
                                    uuid.uuid1())

    def push_to_flush_queue(self, logTuple):
        self.pushCount += 1

        self.flushQueue.put(logTuple, block=False)

    def pop_from_flush_queue(self):
        try:
            item = self.flushQueue.get_nowait()
            self.popCount += 1
        except Queue.Empty:
            item = None

        return item

    def finished_flush(self):
        self.flushQueue.task_done()

    def spawn_flush_thread(self):
        self_ = self

        class flushThread(threading.Thread):
            def __init__(self):
                threading.Thread.__init__(self)

                self.running = True

                self.sysmgr = pycassa.SystemManager(self_.cassandra_host_list[
                    random.randint(0, len(self_.cassandra_host_list) - 1)] +\
                            ':' + str(self_.cassandra_port))

                self.cassandraPool = pycassa.ConnectionPool(
                        keyspace=self_.keyspace,
                        server_list=self_.cassandra_host_list)

            def run(self):
                # Grab something from the queue
                while self.running:
                    # Get a tuple to write
                    logTuple = self_.pop_from_flush_queue()

                    if logTuple:
                        (columnFamily, columnDictionary) = logTuple

                        self.flush_to_cassandra(columnFamily, columnDictionary)

                        self_.finished_flush()

                    # Wait a bit
                    time.sleep(conf.server.flushWaitTime)

            def flush_to_cassandra(self, columnFamily, columnDictionary):
                self_.rowsFlushed += sum([len(val)
                    for val in
                    columnDictionary.values()])
                print "Inserting into column family '{0}'".format(columnFamily)
                pprint.pprint(columnDictionary)
#                if columnFamily not in self.sysmgr.
#                    get_keyspace_column_families(self_.keyspace):
#                    self.sysmgr.create_column_family(
#                                    keyspace=self_.keyspace,
#                                    name=columnFamily,
#                                    comparator_type=UTF8_TYPE,
#                                    key_validation_class=UTF8_TYPE,
#                                    key_alias='data')

                # Connect to Cassandra and insert the data
#                cf = pycassa.ColumnFamily(self.cassandraPool, columnFamily)
#               Original code
#               row = str(int(time.time())) + ':' + gethostname() + ':' +
#                   str(uuid.uuid1())
#               column = str(int(time.time()))
#
#               cf.insert(column,{row : line})
#               cf.insert('lastwrite', {'time' : column })
                try:
                    pass
#                cf.batch_insert(columnDictionary)
                except pycassa.NotFoundException:
                    pass
                except thrift.transport.TTransport.TTransportException:
                    pass

            def shutdown(self):
                self.running = False

        # Start up the flush thread
        self.flushThread = flushThread()
        self.flushThread.start()

    def flush_remainder(self):
        columnFamilies = self.logBuffer.keys()
        for columnFamily in columnFamilies:
            self.push_to_flush_queue((columnFamily, self.logBuffer[columnFamily]))
            del self.logBuffer[columnFamily]

    def flush_log_buffer(self):
        columnFamilies = self.logBuffer.keys()
        for columnFamily in columnFamilies:
            # For each column family to log to
            # Count the rows
            rowCount = 0
            for col in self.logBuffer[columnFamily].values():
                rowCount += len(col.values())

            # It's like this, see?  If we are shutting down,
            # forget the buffering
            # and flush it all to Cassandra!
            if (rowCount >= self.maxLogBufferSize) or not self.running:
                # Write to Cassandra
                try:
                    self.push_to_flush_queue((columnFamily,
                                           self.logBuffer[columnFamily]))
                except Exception, e:
                    pstderr(str(e))
                    pass

                del self.logBuffer[columnFamily]

    def handle_event(self, res):
        fd, eventType = res

        # New data on an existing connection
        if eventType & scribble_server.READ_FLAGS:
            # We have data to read
            (client, address, connectionTime) = self.fdToClientTupleLookup[fd]
            data = client.recv(1024)

            if '' == data:
                # Connection closed; clean up this socket and record the data
                self.openClientCount -= 1

                messageComponents = cPickle.loads(self.clientLogData[fd])

                self.add_data_to_buffer(messageComponents['log'],
                                     messageComponents['cf'],
                                     connectionTime)

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

    def do_work(self):
        """Do does any socket activity needed and then checks if the"""
        """buffered log needs to be dumped"""
        # Do any reading/processing that needs to be done
        try:
            results = self.poller.poll(self.maxPollWait)
        except IOError:
            return

        for res in results:
            try:
                self.handle_event(res)
            except Exception, e:
                pstderr(str(e))

        # Dump the log data if needed
        self.flush_log_buffer()

    def run(self):
        while self.running:
            self.do_work()
            time.sleep(self.intervalBetweenPolls)

    def shutdown(self):
        # In case shutdown has been called more than once (oh signals...)
        if self.shuttingdown or self.shutdownComplete:
            return

        self.shuttingdown = True
        self.running = False

        # Shut down the accepting thread
        self.acceptThread.shutdown()
        self.acceptThread.join()

        # Do one more poll to clean out anything lingering
        self.do_work()

        # Flush anything that's left...
        self.flush_remainder()

        [self.poller.unregister(client) for client in
            [clientTuple[0]
                for clientTuple
                in self.fdToClientTupleLookup.values()]]

        self.fdToClientTupleLookup.clear()
        self.clientLogData.clear()

        # Wait on the flush thread to clear things out of the queue
        self.flushQueue.join()

        # Shut down the flush thread
        self.flushThread.shutdown()
        self.flushThread.join()
        sys.stdout.flush()

        self.shutdownComplete = True
        self.shuttingdown = False

    def report(self):
        print "Server report"
        print "\tServed clients: {0}".format(self.clientCount)
        print "\tStill connected: {0}".format(self.openClientCount)
        print "\tWent wrong: {0}".format(self.wentWrong)
        print "\tTotal pushes to flush queue: {0}".format(self.pushCount)
        print "\tTotal pops from flush queue: {0}".format(self.popCount)
        print "\tTotal rows flushed: {0}".format(self.rowsFlushed)

    def unlabeled_report(self):
        print "{0} {1} {2} {3} {4} {5}".format(
                self.clientCount,
                self.openClientCount,
                self.wentWrong,
                self.pushCount,
                self.popCount,
                self.rowsFlushed)


if __name__ == "__main__":
    try:
        useEpoll = bool(sys.argv[1])
        intervalBetweenPolls = float(sys.argv[2])
        maxPollWait = float(sys.argv[3])
    except:
        print ("usage: scribble_server.py UseEpoll" +
              "intervalBetweenPolls maxPollWait")
        sys.exit(0)

    try:
        srv = scribble_server(useEpoll=True,
                              intervalBetweenPolls=intervalBetweenPolls,
                              maxPollWait=maxPollWait)

        def keyboard_interrupt_handler(signum, frame):
            pstderr("Shutting down")
            srv.shutdown()

        # Catch the interrupt signal, but Resume system calls
        # after the signal is handled
        signal.signal(signal.SIGINT, keyboard_interrupt_handler)
        signal.siginterrupt(signal.SIGINT, False)

        srv.run()
        srv.shutdown()
    except Exception, e:
        print e
        sys.exit(0)
    finally:
        srv.report()

    # Wait for all shutdown to complete
    while not srv.shutdownComplete:
        time.sleep(0.1)
