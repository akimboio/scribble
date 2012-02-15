#!/srv/web-api-config/pythonenv/scribble-production/bin/python

import os
from setuptools import setup


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

try:
    logDir = "/var/log/scribble"
    mode=0777
    # Haven't quite figured out why, but makedirs doesn't use the
    # permissions we ask for.  The Python documentation says
    # it is ignored on some platforms.  How helpful.  In any
    # case, chmod does owrk
    os.makedirs(logDir, mode=mode)
    os.chmod(logDir, mode)
except OSError:
    pass

setup(
    name = "Scribble",
    version = "0.1",
    author = "Josh Marlow",
    author_email = "josh.marlow@retickr.com",
    description = ("""A client/server package for allowing multiple servers
                      to log to a centralized Cassandra database."""),
    license = "Closed",
    keywords = "log cassandra",
    url = "http://about.retickr.com",
    packages=['scribble'],
    long_description=read('README'),
    install_requires=[
        "Pycassa==1.3",
        "Thrift"
        ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: Framework",
        "License :: OSI Approved :: Closed",
        ],
    data_files=[
        ('/srv/scribble/scribble', ['scribble/scribble.conf']) # where to put our lovely config files
        ],
    scripts=[
        'scribble/scribble_client.py',
        'scribble/scribble_server.py',
        'scribble/scribble_tail.py']
)
