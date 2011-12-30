#!/usr/bin/env python
# Created: 12:23 AM, December 10, 2011

import sys
import threading
import random
import subprocess

import scribble_client
import scribble_lib

columnFamilies = ["scribble_test"]

print "Generating block..."
block = "".join(['a' for i in range(1024 * 10)])
print "Built block"


def pstderr(msg):
    sys.stderr.write("\n" + msg + "\n")


def hammer(thread_count):

    class hammerThread(threading.Thread):
        def __init__(self, id_):
            self.id_ = id_
            threading.Thread.__init__(self)

        def run(self):
            message = ("68.169.166.215 - - [08/Nov/2011:11:35:29 -0500] " +
                      "GET /trends/categories?password=secret00&" +
                      'username=joshmarlow@gmail.com HTTP/1.1" 200 6231 ' +
                      "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2)" +
                      "AppleWebKit/535.2 (KHTML, like Gecko)" +
                      "Chrome/15.0.874.106 Safari/535.2")

            message = block

            # We reach deeply into the scribble_client's guts and use
            # it's writeToServer method directory
            try:
                scribbleWriter = scribble_lib.scribble_writer()
                scribble_client.write_to_server(scribbleWriter,
                                              message + " from thread-{0}".\
                                              format(self.id_),
                                              random.choice(columnFamilies))
            except Exception, e:
                pstderr(str(e))

    hammerThreads = [hammerThread(i) for i in range(0, thread_count)]

    [hthr.start() for hthr in hammerThreads]
    [hthr.join() for hthr in hammerThreads]


if __name__ == "__main__":
    runDuplicates = False

    execName = sys.argv[0]
    hammer_count = int(sys.argv[1])

    if len(sys.argv) >= 3:
        duplicates = int(sys.argv[2])
        runDuplicates = True

    try:
        if runDuplicates:
            subHammers = [subprocess.Popen(["python", execName,
                                            str(hammer_count)])
                    for i in range(duplicates)]

            [sh.wait() for sh in subHammers]
        else:
            hammer(hammer_count)
    except KeyboardInterrupt:
        sys.exit(0)
