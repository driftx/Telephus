#!/usr/bin/python
from telephus.protocol import ManagedCassandraClientFactory
from telephus.client import CassandraClient
from telephus.cassandra.ttypes import ColumnPath, ColumnParent
from twisted.internet import defer

HOST = 'localhost'
PORT = 9160
KEYSPACE = 'MyKeyspace'
CF = 'MyColumnFamily'
SCF = 'MySuperColumnFamily'
colname = 'foo'

@defer.inlineCallbacks
def dostuff(client):
    yield client.insert('test', ColumnPath(CF, None, colname), 'testval')
    yield client.insert('test2', ColumnPath(CF, None, colname), 'testval')
    res = yield client.get('test', ColumnPath(CF, None, colname))
    print 'get', res
    res = yield client.get_slice('test', ColumnParent(CF))
    print 'get_slice', res
    res = yield client.multiget(['test', 'test2'], ColumnPath(CF, None, colname))
    print 'multiget', res
    res = yield client.multiget_slice(['test', 'test2'], ColumnParent(CF))
    print 'multiget_slice', res
    res = yield client.get_count('test', ColumnParent(CF))
    print 'get_count', res
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
    testem(c)
    reactor.connectTCP(HOST, PORT, f)
    reactor.run()
