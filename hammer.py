#!/usr/bin/env python
# Created: 12:23 AM, December 10, 2011

import subprocess
import sys
import threading
import time
import random
import os
import scribble_client
import uuid

columnFamilies = ["Users", "Feeds", "Stories"]

def hammer(thread_count):
    failCount = 0
    class hammerThread(threading.Thread):
        def __init__(self, id_):
            self.id_ = id_
            threading.Thread.__init__(self)

        def run(self):
#            time.sleep(random.random() * 3)

            message = '68.169.166.215 - - [08/Nov/2011:11:35:29 -0500] "GET /trends/categories?password=secret00&username=joshmarlow@gmail.com HTTP/1.1" 200 6231 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.874.106 Safari/535.2'

            # We reach deeply into the scribble_client's guts and use
            # it's writeToServer method directory
            try:
                scribble_client.writeToServer(message + " from thread-{0}".\
                                              format(self.id_),
                                              random.choice(columnFamilies))
            except Exception, e:
                print e

    hammerThreads = [hammerThread(i) for i in range(0, thread_count)]
    
    [hthr.start() for hthr in hammerThreads]
    [hthr.join() for hthr in hammerThreads]
    
if __name__ == "__main__":
    sub_proc_count = int(sys.argv[1])

#    print "Hammering with {0} threads".format(sub_proc_count)

    try:
        hammer(sub_proc_count)
    except KeyboardInterrupt:
        sys.exit(0);
