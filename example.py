#!/usr/bin/python
from telephus.protocol import ManagedCassandraClientFactory
from telephus.client import CassandraClient
from telephus.cassandra.ttypes import ColumnPath, ColumnParent
from twisted.internet import defer

HOST = 'localhost'
PORT = 9160
KEYSPACE = 'Keyspace1'
CF = 'Standard1'
SCF = 'Super1'
colname = 'foo'
scname = 'bar'

@defer.inlineCallbacks
def dostuff(client):
    # you can pass ttypes objects directly
    yield client.insert('test', ColumnPath(CF, None, colname), 'testval')
    # or not
    yield client.insert('test2', CF, 'testval', column=colname)
    yield client.insert('test', SCF, 'testval', column=colname, super_column=scname)
    res = yield client.get('test', CF, column=colname)
    print 'get', res
    res = yield client.get('test', SCF, column=colname, super_column=scname)
    print 'get (super)', res
    res = yield client.get_slice('test', CF)
    print 'get_slice', res
    res = yield client.multiget(['test', 'test2'], CF, column=colname)
    print 'multiget', res
    res = yield client.multiget_slice(['test', 'test2'], CF)
    print 'multiget_slice', res
    res = yield client.get_count('test', CF)
    print 'get_count', res
    # batch insert will figure out if you're trying a CF or SCF
    # from the data structure
    res = yield client.batch_insert('test', CF, {colname: 'bar'})
    print "batch_insert", res
    res = yield client.batch_insert('test', SCF, {'foo': {colname: 'bar'}})
    print "batch_insert", res


if __name__ == '__main__':
    from twisted.internet import reactor
    from twisted.python import log
    import sys
    log.startLogging(sys.stdout)

    f = ManagedCassandraClientFactory()
    c = CassandraClient(f, KEYSPACE)
    dostuff(c)
    reactor.connectTCP(HOST, PORT, f)
    reactor.run()
