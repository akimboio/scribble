#!/usr/bin/python

"""
tail.py - python log tail utility from cassandra

@author: Josh Marlow
@organization: retickr
@contact: josh.marlow+scribble@retickr.com

usage:
   tail.py <column family>

   where
       <column family> is the name of the column family, probably
       corresponding to vhost

"""

import pycassa
import sys
import time
import thrift
import os.path
import json

with open(os.path.join(os.path.dirname(__file__), "scribble.conf")) as f:
    __conf__ = json.loads(f.read())

def tail():
    ii = 0
    start_time = int(cf.get('lastwrite')['time'])

    while True:
        try:
            rows = cf.get(str(start_time + ii))

            for jj in rows.values():
                sys.stdout.writelines(jj + '\n')
                sys.stdout.flush()

        except thrift.transport.TTransport.TTransportException:
            pass
        except KeyboardInterrupt:
            exit()
        except Exception, e:
            #print e
            pass
        except:
            pass

        try:
            time.sleep(1)
            ii = ii + 1
        except KeyboardInterrupt:
            exit()

if __name__ == "__main__":
    if len(sys.argv) >= 2:
        column_family = sys.argv[1]
    else:
        sys.exit("You must supply a column family")

    keyspace = __conf__["cassandra"]["keyspace"]
    server_list = __conf__["cassandra"]["hosts"]
    server_port = __conf__["cassandra"]["server_port"]
    pool = pycassa.ConnectionPool(keyspace=keyspace, server_list=server_list)
    cf = pycassa.ColumnFamily(pool, column_family)

    tail()
