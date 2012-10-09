#!/usr/bin/python
from telephus.protocol import ManagedCassandraClientFactory
from telephus.client import CassandraClient
from telephus.cassandra.ttypes import Column, KsDef, CfDef
from twisted.internet import defer

HOST = 'localhost'
PORT = 9160
KEYSPACE = 'Keyspace1'
CF = 'Standard1'
colname = 'foo'
scname = 'bar'

@defer.inlineCallbacks
def dostuff(client):
    yield client.insert(key='test', column_family=CF, value='testval', column=colname)

    res = yield client.get(key='test', column_family=CF, column=colname)
    print 'get', res

    res = yield client.get_slice(key='test', column_family=CF)
    print 'get_slice', res

    res = yield client.multiget(keys=['test', 'test2'], column_family=CF, column=colname)
    print 'multiget', res

    res = yield client.multiget_slice(keys=['test', 'test2'], column_family=CF)
    print 'multiget_slice', res

    res = yield client.get_count(key='test', column_family=CF)
    print 'get_count', res

    # batch insert will figure out if you're trying a CF or SCF
    # from the data structure
    res = yield client.batch_insert(key='test', column_family=CF, mapping={colname: 'bar'})
    print "batch_insert", res

    # with ttypes, you pass a list as you would for raw thrift
    # this way you can set custom timestamps
    cols = [Column(colname, 'bar', 1234), Column('bar', 'baz', 54321)]
    res = yield client.batch_insert(key='test', column_family=CF, mapping=cols)
    print "batch_insert", res

    client.system_drop_keyspace('Keyspace1')


@defer.inlineCallbacks
def setup_schema(client):
    print "setting up schema"
    ks_defs = yield client.describe_keyspaces()
    if 'Keyspace1' not in [k.name for k in ks_defs]:
        yield client.system_add_keyspace(KsDef('Keyspace1', 'SimpleStrategy', {'replication_factor': '1'}, cf_defs=[]))
        yield client.set_keyspace('Keyspace1')
        yield client.system_add_column_family(CfDef('Keyspace1', 'Standard1'))
        print "schema created"
    else:
        print "schema already exists"
        yield client.set_keyspace('Keyspace1')


@defer.inlineCallbacks
def main():
    sasl_kwargs = {
            "host": "thobbs-laptop2",
            "service": "host",
            "mechanism": "GSSAPI"}
    f = ManagedCassandraClientFactory(keyspace='system', sasl_kwargs=sasl_kwargs)
    reactor.connectTCP(HOST, PORT, f)
    yield f.deferred
    c = CassandraClient(f)

    yield setup_schema(c)
    yield dostuff(c)


if __name__ == '__main__':
    from twisted.internet import reactor
    from twisted.python import log
    import sys
    log.startLogging(sys.stdout)
    main()
    reactor.run()
