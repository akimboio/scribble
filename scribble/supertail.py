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
    start_times = []

    for cf in cf_list:
        start_times.append(int(cf.get('lastwrite')['time']))

   
    while True:
    
        for i in range(len(cf_list)):
        
	    cf = cf_list[i]

            try:
                rows = cf.get(str(start_times[i] + ii))

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
    usage = "usage: {0} <column-family> [column-family]".format(sys.argv[0])

    if len(sys.argv) >= 2:
        column_families = sys.argv[1:]
    else:
	print usage
        sys.exit("You must supply a column family")

    keyspace = __conf__["cassandra"]["keyspace"]
    server_list = __conf__["cassandra"]["hosts"]
    server_port = __conf__["cassandra"]["server_port"]
    pool = pycassa.ConnectionPool(keyspace=keyspace, server_list=server_list)
    cf_list = [pycassa.ColumnFamily(pool, name) for name in column_families]
    
    tail()
