#!/usr/bin/env python
import os
import signal
import time
import sys
import subprocess
import datetime

import scribble_server
import hammer

# Number of times to run each experiment
sampleCount = 50
hammerCount = 900

blankReturns = 0
totalRuns = 0

try:
    gitHash = subprocess.check_output(['git','log','-1']).split(' ')[1].split('\n')[0]
except:
    gitHash = 'Unknown'

print "Run date: {0}\ncurrent git hash: {1}\n".format(str(datetime.datetime.today()), gitHash)

try:
    for useEpoll in [False, True]:
        for maxPollWaitInt in [1]:#range(10, 0, -1):
            for intervalBetweenPollsInt in [1]:#range(10, 0, -1):
                # For each parameter set
                maxPollWait = maxPollWaitInt * 0.01
                intervalBetweenPolls = intervalBetweenPollsInt * 0.01

                data = [0 for i in range(6)]

                print "Experiment useEpoll: {0}, maxPollWait:{1}, intervalBetweenPolls:{2}".format(useEpoll, maxPollWait, intervalBetweenPolls)

                i = 0

                while i < sampleCount:
                    sys.stdout.write("\t\tPerforming sample {0}/{1}...".format(i+1, sampleCount))
                    sys.stdout.flush()
                    totalRuns += 1

                    # Wait a bit for everything to settle down...
                    time.sleep(1)

                    serverHandle = subprocess.Popen(["python", "scribble_server.py", str(useEpoll), str(intervalBetweenPolls), str(maxPollWait)], stdout=subprocess.PIPE)

                    # Wait a sec for the server to spin up
                    time.sleep(1)

                    hammerHandle = subprocess.Popen(["python", "hammer.py",  str(hammerCount)])

                    hammerHandle.wait()

                    serverHandle.send_signal(signal.SIGINT)

                    serverHandle.wait()

                    server_report = serverHandle.stdout.read()

                    if len(server_report) == 0:
                        # Not sure why, but sometimes this happens.  Try again!
                        blankReturns += 1
                        continue

                    server_floats = [float(field) for field in server_report.split(' ')]

                    print "  {0}".format(server_floats)

                    data = [data[j] + server_floats[j] for j in range(len(data))]

                    i += 1

                data = [datum/float(sampleCount) for datum in data]

                print "\taverage: {0}".format(data)

except KeyboardInterrupt:
    try:
        print "Shutting down nicely"
        try:
            serverHandle.send_signal(signal.SIGINT)
            serverHandle.wait()
        except: pass

        try:
            hammerHandle.send_signal(signal.SIGINT)
            hammerHandle.wait()
        except: pass
    except KeyboardInterrupt:
        print "Killing time"
        try: serverHandle.kill()
        except: pass

        try: hammerHandle.kill()
        except: pass

print "blankReturns: {0}/{1} ({2} %)".format(blankReturns, totalRuns, blankReturns/float(totalRuns) * 100)
