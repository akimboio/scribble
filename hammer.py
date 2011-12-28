#!/usr/bin/env python
# Created: 12:23 AM, December 10, 2011

import sys
import threading
import random
import scribble_client
import scribble_lib

columnFamilies = ["scribble_test"]


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

            # We reach deeply into the scribble_client's guts and use
            # it's writeToServer method directory
            try:
                scribbleWriter = scribble_lib.scribble_writer()
                scribble_client.write_to_server(scribbleWriter,
                                              message + " from thread-{0}".\
                                              format(self.id_),
                                              random.choice(columnFamilies))
            except Exception, e:
#                print e
                pass

    hammerThreads = [hammerThread(i) for i in range(0, thread_count)]

    [hthr.start() for hthr in hammerThreads]
    [hthr.join() for hthr in hammerThreads]


if __name__ == "__main__":
    hammer_count = int(sys.argv[1])
    duplicates = int(sys.argv[2])

#    print "Hammering with {0} threads".format(hammer_count)

    try:
        for i in range(duplicates):
            threading.Thread(target=(lambda: hammer(hammer_count))).start()

#            if (i % 100) == 0:
#                print "{0} of {1} threads dispatched".format(i, duplicates)
    except KeyboardInterrupt:
        sys.exit(0)
