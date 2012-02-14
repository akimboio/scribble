#!/usr/bin/env python
"""

Scribble server

The scrible server functions as a buffer between scribble clients
which are processes receiving writes from a logging process
and the Cassandra cluster itself which persistently logs
data. It is neccassary to provide this client side buffer
to reduce the number of small writes Cassandra receives
and not overwhelm it.

"""

import socket
import select
import time
import json
import random
import os
import os.path
import threading
import sys
import signal
import Queue

import pycassa
import thrift

import scribble.scribble_lib as scribble_lib


__conf__ = scribble_lib.load_config_file()


__license__ = "Copyright (c) 2012, Retickr, LLC"
__organization__ = "Retickr, LLC"
__authors__ = [
    "Josh Marlow <josh.marlow+scribble@retickr.com>",
    "Micah Hausler <micah.hausler+scribble@retickr.com>",
    "Adam Haney <adam.haney+scribble@retickr.com"
    ]


def pstderr(msg):
    """
    Print to stderr.  Simple helper function to keep from cluttering
    stdout
    """
    sys.stderr.write("\n" + msg + "\n")


class scribble_server:
    """
    This is the scribble server.  It functions as a long running
    process that scribble clients can write data to.  Periodically the
    server writes it's data to Cassandra.  At shutdown, it takes a few
    seconds to write anything remaning in it's buffers to Cassandra.
    """

    POLL_READ_FLAGS = select.POLLIN | select.POLLPRI
    POLL_CLOSE_FLAGS = select.POLLERR | select.POLLHUP | select.POLLNVAL

    EPOLL_READ_FLAGS = select.EPOLLIN | select.EPOLLPRI
    EPOLL_CLOSE_FLAGS = select.EPOLLERR | select.EPOLLHUP

    READ_FLAGS = POLL_READ_FLAGS
    CLOSE_FLAGS = POLL_CLOSE_FLAGS

    def __init__(self):
        # The state of the server and the step in the shutdown process
        self.running = True
        self.shutdownComplete = False
        self.shuttingdown = False

        # Stats for generating the report
        self.clientCount = 0
        self.openClientCount = 0
        self.wentWrong = 0
        self.flushPushCount = 0
        self.flushPopCount = 0
        self.rowsFlushed = 0

        self.conf = __conf__

        # Do misc configuration
        self.useEpoll = True
        self.host = self.conf["server"]["host"]
        self.port = int(self.conf["server"]["port"])
        self.maxConnectionBacklog = socket.SOMAXCONN
        self.intervalBetweenPolls =\
            float(self.conf["server"]["intervalBetweenPolls"])
        self.maxPollWait = float(self.conf["server"]["maxPollWait"])
        self.maxLogBufferSize = int(self.conf["server"]["maxLogBufferSize"])

        self.cassandra_host_list = self.conf["cassandra"]["hosts"]
        self.cassandra_port = int(self.conf["cassandra"]["server_port"])

        self.flushQueue = Queue.Queue()

        self.spawn_flush_thread()

        # set up networking
        self.setup_client_limiting()
        self.setup_networking()

        # Keep track of how long since we last did a full flush
        self.lastFullFlushTime = time.time()

    def setup_client_limiting(self):
        # So, we don't want to have more file descriptors open
        # than we can handle (due to open file limitations), so
        # we keep track of how many files we have open and don't
        # accept more than that.  This adaptively limits the number
        # of connections we can serve.  Because we configure listen
        # to allow the maximum number of connections in the backlog, we
        # have the OS buffering connections for us
        maxDescriptors = int(os.sysconf("SC_OPEN_MAX"))
        openDescriptors = os.listdir("/proc/self/fd")

        # I've not figured out a way to determine how many connections
        # pycassa may need to function, so we'll reserve a buffer of 10
        # file descriptors
        self.maxClients = maxDescriptors - len(openDescriptors) - 2 - 10

        print """Preparing to support {0} concurrent clients""".\
                format(self.maxClients)
        self.clientLimitSemaphore = threading.Semaphore(value=self.maxClients)

    def setup_networking(self):
        """
        Create the polling object, and spawn the listening/accepting
        thread
        """

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
            """
            Embedded class that implements the server's socket
            listening and accepting thread
            """

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

                try:
                    self.listenSocket.bind((self_.host, self_.port))
                    self.listenSocket.listen(self_.maxConnectionBacklog)
                except socket.error:
                    # Another server is already listening on this port,
                    # so exit
                    print "Socket is already in use; shutting down"
                    self_.force_shutdown()

            def run(self):
                """
                Begin listening and accepting socket connections
                """

                while self.running:
                    try:
                        client, clientAddress = self.listenSocket.accept()

                        if self.running:
                            # Only accept the new connection if we are
                            # still running
                            self_.setup_new_client(client, clientAddress)
                    except Exception, e:
                        pstderr("Exception {0}, {1}, {2}".format(e,
                        type(e).__name__, len(self_.fdToClientTupleLookup)))

                self.listenSocket.close()

            def shutdown_accept_thread(self):
                """
                Shutdown (gracefully) the accepting thread.  To do this,
                we make a connection to the thread and then close it to
                trigger the accept call
                """

                self.running = False

                # Connect so that accept will return...
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                try:
                    s.connect((self_.host, self_.port))
                except:
                    pass

                s.close()

        self.acceptThread = acceptConnectionThread()
        self.acceptThread.start()

    def setup_new_client(self, client, clientAddress):
        """
        Set up the appropriate data structures for this client and
        start polling it
        """

        self.clientLimitSemaphore.acquire()

        client.setblocking(0)

        clientFd = client.fileno()

        self.fdToClientTupleLookup[clientFd] =\
                (client, clientAddress, str(int(time.time())))
        self.clientLogData[clientFd] = ''
        self.poller.register(client,
                              scribble_server.READ_FLAGS |
                              scribble_server.CLOSE_FLAGS)

        self.clientCount += 1
        self.openClientCount += 1

    def cleanup_old_client(self, client):
        """
        Clean out the old data structures associated with this client
        """

        clientFd = client.fileno()

        del self.clientLogData[clientFd]
        del self.fdToClientTupleLookup[clientFd]

        self.poller.unregister(client)
        client.shutdown(socket.SHUT_RDWR)
        client.close()

        self.openClientCount -= 1

        # Since we are closing down this file descriptor, we can now afford
        # to open up a new one
        self.clientLimitSemaphore.release()

    def add_data_to_buffer(self, data, connectionTime, superColumn=False):
        """
        Add this write job to the log buffer so that it may be flushed in
        the future.
        """

        keyspace = data["keyspace"]
        columnFamily = data["columnFamily"]
        rowKey = data["rowKey"]
        if superColumn:
            superColumnName = data["superColumnName"]
        columnName = data["columnName"]
        columnValue = data["value"]

        # Make sure we have the needed entries in the dictionary
        def make_key_if_needed(dict_, key):
            dict_[key] = dict_.get(key, {})

        make_key_if_needed(self.logBuffer, keyspace)
        make_key_if_needed(self.logBuffer[keyspace], columnFamily)
        make_key_if_needed(self.logBuffer[keyspace][columnFamily], rowKey)

        if superColumn:
            make_key_if_needed(self.logBuffer[keyspace][columnFamily][rowKey],
                            superColumnName)
            make_key_if_needed(
               self.logBuffer[keyspace][columnFamily][rowKey][superColumnName],
                           columnName)

            self.logBuffer[keyspace][columnFamily][rowKey][superColumnName]\
                            [columnName] = columnValue
        else:
            make_key_if_needed(self.logBuffer[keyspace][columnFamily][rowKey],
                            columnName)

            self.logBuffer[keyspace][columnFamily][rowKey][columnName] =\
                            columnValue

    def push_to_flush_queue(self, logTuple):
        """
        Push this write job to the flush queue so that it will be
        written to Cassandra
        """

        self.flushPushCount += 1

        self.flushQueue.put(logTuple, block=False)

    def pop_from_flush_queue(self):
        """
        Pop a write job from the flush queue so that we may
        write it to Cassandra
        """
        try:
            item = self.flushQueue.get(block=True, timeout=0.5)
            self.flushPopCount += 1
        except Queue.Empty:
            item = None

        return item

    def finished_flush(self):
        """
        Acknowledges that a flush is complete.
        """
        self.flushQueue.task_done()

    def spawn_flush_thread(self):
        """
        Spawn a thread that pops write jobs off of the flush queue
        and writes the to Cassandra
        """
        self_ = self

        class flushThread(threading.Thread):
            """
            Implements the thread that repeatedly pulls write jobs from
            the flush queue and flushes them to cassandra
            """
            def __init__(self):
                threading.Thread.__init__(self)

                self.running = True

                self.sysmgr = pycassa.SystemManager("{0}:{1}".\
                        format(random.choice(self_.cassandra_host_list),
                               self_.cassandra_port))

            def run(self):
                """
                Loop as long as the server is running and grab write
                jobs and flush them to cassandra
                """
                while self.running:
                    # Get a write job
                    logTuple = self_.pop_from_flush_queue()

                    if logTuple:
                        (keyspace, columnFamily, columnDictionary) = logTuple

                        retry = True

                        try:
                            retry = not self.flush_to_cassandra(keyspace,
                                    columnFamily, columnDictionary)
                        except Exception, e:
                            pstderr(str(e))
                        finally:
                            # Note: we want to finish the flush even if it
                            # failed; otherwise we don't call task_done enough
                            # times...
                            self_.finished_flush()

                        if retry:
                            # Could not flush to cassandra so add it
                            # back to the queue
                            pstderr("Flush was not successful so add it back"
                                    "to the queue...")
                            self_.push_to_flush_queue(logTuple)

            def flush_to_cassandra(self, keyspace, columnFamily,
                    columnDictionary):
                """
                Write this data to Cassandr now
                """

                if keyspace not in self.sysmgr.list_keyspaces():
                    # Create the keyspace if it does not yet exist
                    pstderr("Creating keyspace {0}...".format(keyspace))
                    self.sysmgr.create_keyspace(
                        keyspace,
                        strategy_options=self.conf["cassandra"]\
                                ["new_keyspace_strategy_option"])

                cassandraPool = pycassa.ConnectionPool(
                        keyspace=keyspace,
                        server_list=self_.cassandra_host_list)

                if columnFamily not in self.sysmgr.\
                    get_keyspace_column_families(keyspace):
                    # Create the column family if it does not yet exist
                    self.sysmgr.create_column_family(
                                    keyspace=keyspace,
                                    name=columnFamily,
                                    comparator_type=pycassa.UTF8_TYPE,
                                    key_validation_class=pycassa.UTF8_TYPE,
                                    key_alias='data')

                # Connect to Cassandra and insert the data
                cf = pycassa.ColumnFamily(cassandraPool, columnFamily)

                writeResult = False
                try:
                    rowCount = sum([len(val)
                        for val in
                        columnDictionary.values()])

                    pstderr("Batch inserting {0} rows into keyspace: '{1}'"
                            "and column family: '{2}'".format(rowCount,
                                keyspace, columnFamily))

                    cf.batch_insert(columnDictionary,
                            write_consistency_level = \
                                    pycassa.ConsistencyLevel.QUORUM)

                    self_.rowsFlushed += rowCount

                    writeResult = True

                except pycassa.NotFoundException:
                    pass
                except thrift.transport.TTransport.TTransportException:
                    pass
                finally:
                    cassandraPool.dispose()

                return writeResult

            def shutdown_flush_thread(self):
                """Shutdown the flush thread"""
                self.running = False

                self.sysmgr.close()

        # Start up the flush thread
        self.flushThread = flushThread()
        self.flushThread.start()

    def flush_remainder(self):
        """
        Flush the remaining data in the buffer to Casssandra
        """

        keyspaces = self.logBuffer.keys()

        for keyspace in keyspaces:
            # Flush each keyspace
            columnFamilies = self.logBuffer[keyspace].keys()
            for columnFamily in columnFamilies:
                self.push_to_flush_queue((keyspace, columnFamily,
                    self.logBuffer[keyspace][columnFamily]))
                del self.logBuffer[keyspace][columnFamily]

    def flush_log_buffer(self):
        """
        Look at all of the columns and flush any that have a lot of data
        """

        keyspaces = self.logBuffer.keys()

        for keyspace in keyspaces:
            # For each keyspace to log to
            columnFamilies = self.logBuffer[keyspace].keys()

            for columnFamily in columnFamilies:
                # For each column family to log to
                # Count the rows
                rowCount = 0
                for col in self.logBuffer[keyspace][columnFamily].values():
                    rowCount += len(col.values())

                # It's like this, see?  If we are shutting down,
                # forget the buffering
                # and flush it all to Cassandra!
                if (rowCount >= self.maxLogBufferSize) or not self.running:
                    # Write to Cassandra
                    try:
                        self.push_to_flush_queue((keyspace, columnFamily,
                                       self.logBuffer[keyspace][columnFamily]))
                    except Exception, e:
                        pstderr(str(e))
                        pass

                    del self.logBuffer[keyspace][columnFamily]

    def handle_event(self, res):
        """
        Something has happened on this socket; read from it or close it
        """

        fd, eventType = res

        # New data on an existing connection
        if eventType & scribble_server.READ_FLAGS:
            # We have data to read
            (client, address, connectionTime) = self.fdToClientTupleLookup[fd]
            try:
                data = client.recv(1024)
            except Exception, e:
                print e
                pass

            if '' == data:
                # Connection closed; clean up this socket and record the data
                if len(self.clientLogData[fd]) > 0:
                    message = json.loads(self.clientLogData[fd])

                    self.add_data_to_buffer(message, connectionTime)

                self.cleanup_old_client(client)
            else:
                self.clientLogData[fd] += data
        elif eventType & scribble_server.CLOSE_FLAGS:
            # Something went wrong
            (client, address, connectionTime) = self.fdToClientTupleLookup[fd]

            self.cleanup_old_client(client)

            self.wentWrong += 1
        else:
            print "****** ELSE!"

    def do_work(self):
        """
        Do any socket activity needed and then checks if the
        buffered log needs to be dumped
        """

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
        if (time.time() - self.lastFullFlushTime) >=\
            float(self.conf["server"]["maxFlushInterval"]):
            # It's been a while since we last did a full flush, so do it now
            self.flush_remainder()
            self.lastFullFlushTime = time.time()
        else:
            # Flush a bit if we have a lot of data
            self.flush_log_buffer()

    def run(self):
        """
        Go through the motions of running the server, reading and
        flushing data as appropriate
        """
        while self.running:
            self.do_work()
            time.sleep(self.intervalBetweenPolls)

    def shutdown(self):
        """
        Shut down the server, writing any remaining log data to Cassandra
        """

        # In case shutdown has been called more than once (oh signals...)
        if self.shuttingdown or self.shutdownComplete:
            return

        self.shuttingdown = True
        self.running = False

        # Shut down the accepting thread
        self.acceptThread.shutdown_accept_thread()
        self.acceptThread.join()

        # Do one more poll to clean out anything lingering
        self.do_work()

        # Flush anything that's left...
        self.flush_remainder()

        self.fdToClientTupleLookup.clear()
        self.clientLogData.clear()

        # Close any remaining sockets
        [self.poller.unregister(client) and
                client.shutdown(socket.SHUT_RDWR) and
                client.close()
                for client in
            [clientTuple[0]
                for clientTuple
                in self.fdToClientTupleLookup.values()]]

        # Wait on the flush thread to clear things out of the queue
        print "Waiting on flush queue ({0} jobs pending)...".\
                format(self.flushPushCount - self.flushPopCount)
        self.flushQueue.join()

        # Shut down the flush thread
        print "Shutting down flush thread"
        self.flushThread.shutdown_flush_thread()
        self.flushThread.join()
        sys.stdout.flush()

        self.shutdownComplete = True
        self.shuttingdown = False

    def force_shutdown(self):
        self.shutdownComplete = True
        self.shuttingdown = False

        self.flushThread.shutdown_flush_thread()
        self.acceptThread.shutdown_accept_thread()

    def pending_write_jobs(self):
        return self.flushPushCount - self.flushPopCount

    def report(self):
        """
        Print a human readable report of various server stats
        """

        print "Server report"
        print "\tServed clients: {0}".format(self.clientCount)
        print "\tStill connected: {0}".format(self.openClientCount)
        print "\tWent wrong: {0}".format(self.wentWrong)
        print "\tTotal pushes to flush queue: {0}".format(self.flushPushCount)
        print "\tTotal pops from flush queue: {0}".format(self.flushPopCount)
        print "\tTotal rows flushed: {0}".format(self.rowsFlushed)

    def unlabeled_report(self):
        """
        Print an unlabled (ie, not human readable) report of various
        server stats.  This output is easier for machines to parse
        than the output of report
        """

        print "{0} {1} {2} {3} {4} {5} {6} {7}".format(
                self.clientCount,
                self.openClientCount,
                self.wentWrong,
                self.flushPushCount,
                self.flushPopCount,
                self.rowsFlushed)


if __name__ == "__main__":
    srv = None

    if len(sys.argv) >= 3:
        useEpoll = bool(sys.argv[1])
        intervalBetweenPolls = float(sys.argv[2])
        maxPollWait = float(sys.argv[3])

    try:
        srv = scribble_server()

        # Catch the interrupt signal, but resume system calls
        # after the signal is handled
        def keyboard_interrupt_handler(signum, frame):
            pstderr("Shutting down...")
            pstderr("Flushing {0} write jobs to database...".\
                    format(srv.pending_write_jobs()))

            # It's *possible* that the server could hang while being
            # shutdown; in that case, we want to be able to forcefully exit
            # (after verification)
            def exit_now(signum, frame):
                yn = str(raw_input("\n{0} pending write jobs;"\
                        "quit anyway (y/n)?  ".\
                        format(srv.pending_write_jobs())))

                if yn in ['y', 'Y', "yes", "YES", "Yes"]:
                    pstderr("Forcing shutdown")
                    srv.force_shutdown()
                    exit(0)

            signal.signal(signal.SIGINT, exit_now)

            srv.shutdown()

        signal.signal(signal.SIGINT, keyboard_interrupt_handler)
        signal.siginterrupt(signal.SIGINT, False)

        srv.run()
        srv.shutdown()
    except Exception, e:
        print "Error in running: {0}".format(e)
        sys.exit(0)
    finally:
        if srv:
#           srv.unlabeled_report()
            srv.report()

    # Wait for all shutdown to complete
    while not srv.shutdownComplete:
        time.sleep(0.1)
