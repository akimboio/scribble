import os
from setuptools import setup


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "Scribble",
    version = "0.1",
    author = "Josh Marlow",
    author_email = "josh.marlow@retickr.com",
    description = ("""A client/server package for allowing multiple servers to log to a centralized Cassandra database."""),
    license = "Closed",
    keywords = "log cassandra",
    url = "http://about.retickr.com",
    packages=['scribble'],
    long_description=read('README'),
    install_requires=[],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: Framework",
        "License :: OSI Approved :: Closed",
        ]
)

