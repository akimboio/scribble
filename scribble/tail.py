#! /usr/bin/python

"""
tail.py - python log tail utility from cassandra

@author: Josh Marlow
@organization: retickr
@contact: josh.marlow+scribble@retickr.com

usage:
   tail.py <column family>

   where
       <column family> is the name of the column family, probably corresponding to vhost

"""

import pycassa, sys, time
import thrift

import scribble.scribble_config as conf

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
            ii=ii+1
        except KeyboardInterrupt:
            exit()

if __name__ == "__main__":
    tail()
