#! /usr/bin/python
""" 
scribble.py - python logging utility to cassandra

@organization: retickr
@contact: micah.hausler+scribble@retickr.com

usage: 
		<STDIN> | scribble.py <column family>
			or in httpd.conf
		CustomLog "|| /path/to/scribble.py <column family>" combined
			Double pipe is necessisary to not kill your cpu
			See http://httpd.apache.org/docs/current/logs.html#piped

		where 
			<STDIN> is a process, ex: "tail -f", apache log
			<column family> is the name of the column family, probably corresponding to vhost
"""

import pycassa, os, sys, time, uuid, random, conf
from time	import strftime
from socket	import gethostname
from pycassa.system_manager import *
import thrift

keyspace 	= conf.cassandra.keyspace
server_list	= conf.cassandra.hosts
server_port	= conf.cassandra.server_port

if len(sys.argv) >= 2:
	column_family = sys.argv[1]
else:
	sys.exit("You must supply a column family")

sysmgr= SystemManager(server_list[random.randint(0,len(server_list)-1)]+':'+str(server_port))

if column_family not in sysmgr.get_keyspace_column_families(keyspace):
	sysmgr.create_column_family(
					keyspace=keyspace,
					name=column_family,
					comparator_type=UTF8_TYPE,
					key_validation_class=UTF8_TYPE,
					key_alias='data')

def insert(line, cf):
	row = gethostname() + str(uuid.uuid1()) + strftime('%S')
	row = str(int(time.time()))+':'+gethostname() +':'+ str(uuid.uuid1()) 
	cf.insert(column,{row : line})
	cf.insert('lastwrite', {'time' : column })
	
def run(): 
    column = str(int(time.time()))
    pool = pycassa.ConnectionPool( keyspace	= keyspace, server_list	= server_list)
    cf = pycassa.ColumnFamily(pool,column_family)
    while True:
        try:
		    line = sys.stdin.readline().rstrip()
		    if line:
		    	insert(line, cf)
		    	#print line
		    else:
		    	pass
        except pycassa.NotFoundException:
            pass
        except thrift.transport.TTransport.TTransportException:
            pass
        except Exception, e:
            print e 
            pass
run()

