#! /usr/bin/python

""" 
tail.py - python log tail utility from cassandra

@author: Micah Hausler
@organization: retickr
@contact: micah.hausler+scribble@retickr.com

usage: 
   tail.py <column family>

   where
       <column family> is the name of the column family, probably corresponding to vhost

"""

import pycassa, sys, time, conf
from pycassa.system_manager import *

keyspace 	= conf.cassandra.keyspace
server_list	= conf.cassandra.hosts
server_port	= conf.cassandra.server_port

if len(sys.argv) >= 2:
    column_family = sys.argv[1]
else:
    sys.exit("You must supply a column family")

pool = pycassa.ConnectionPool( keyspace	= keyspace, server_list	= server_list)
cf = pycassa.ColumnFamily(pool,column_family)

def tail():
    ii=0
    start_time = int(cf.get('lastwrite')['time'])
    while True:
        try:
            rows = cf.get(str(start_time+ii))
            for i in rows.values():
                sys.stdout.writelines(i+'\n')
                sys.stdout.flush()
        except pycassa.NotFoundException:
            pass
        except pycassa.TTransportException:
            pass
        time.sleep(1)
        ii=ii+1

tail()
