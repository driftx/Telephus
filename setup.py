#!/usr/bin/python

from distutils.core import setup
setup(
    name='telephus',
    version='0.7',
    description='connection pooled, low-level client API for Cassandra in Twisted python',
    author='brandon@faltering.com',
    url='http://github.com/driftx/Telephus',
    packages=['telephus', 'telephus.cassandra']
)
