from twisted.trial import unittest
from twisted.python.failure import Failure
from twisted.internet import defer, reactor
from telephus.protocol import ManagedCassandraClientFactory
from telephus.client import CassandraClient
from telephus.cassandra.ttypes import *
import os

CONNS = 5

HOST = os.environ.get('CASSANDRA_HOST', 'localhost')
PORT = 9160
KEYSPACE = 'Keyspace1'
T_KEYSPACE = 'TransientKeyspace'
CF = 'Standard1'
SCF = 'Super1'
T_CF = 'TransientCF'
T_SCF = 'TransientSCF'
COLUMN = 'foo'
COLUMN2 = 'foo2'
SCOLUMN = 'bar'

class CassandraClientTest(unittest.TestCase):
    def setUp(self):
        self.cmanager = ManagedCassandraClientFactory(keyspace=KEYSPACE)
        self.client = CassandraClient(self.cmanager)
        for i in xrange(CONNS):
            reactor.connectTCP(HOST, PORT, self.cmanager)
        return self.cmanager.deferred
    
    @defer.inlineCallbacks
    def tearDown(self):
        yield self.client.remove('test', CF)
        yield self.client.remove('test2', CF)
        yield self.client.remove('test', SCF)
        yield self.client.remove('test2', SCF)
        self.cmanager.shutdown()
        for c in reactor.getDelayedCalls():
            c.cancel()
        reactor.removeAll()
    
    @defer.inlineCallbacks
    def test_insert_get(self): 
        yield self.client.insert('test', CF, 'testval', column=COLUMN)
        yield self.client.insert('test2', CF, 'testval2', column=COLUMN)
        yield self.client.insert('test', SCF, 'superval', column=COLUMN, super_column=SCOLUMN)
        yield self.client.insert('test2', SCF, 'superval2', column=COLUMN,
                                 super_column=SCOLUMN)
        res = yield self.client.get('test', CF, column=COLUMN)
        self.assert_(res.column.value == 'testval')
        res = yield self.client.get('test2', CF, column=COLUMN)
        self.assert_(res.column.value == 'testval2')
        res = yield self.client.get('test', SCF, column=COLUMN, super_column=SCOLUMN)
        self.assert_(res.column.value == 'superval')
        res = yield self.client.get('test2', SCF, column=COLUMN, super_column=SCOLUMN)
        self.assert_(res.column.value == 'superval2')

    @defer.inlineCallbacks
    def test_batch_insert_get_slice_and_count(self):
        yield self.client.batch_insert('test', CF,
                                       {COLUMN: 'test', COLUMN2: 'test2'})
        yield self.client.batch_insert('test', SCF,
                               {SCOLUMN: {COLUMN: 'test', COLUMN2: 'test2'}})
        res = yield self.client.get_slice('test', CF, names=(COLUMN, COLUMN2)) 
        self.assert_(res[0].column.value == 'test')
        self.assert_(res[1].column.value == 'test2')
        res = yield self.client.get_slice('test', SCF, names=(COLUMN, COLUMN2),
                                          super_column=SCOLUMN)
        self.assert_(res[0].column.value == 'test')
        self.assert_(res[1].column.value == 'test2')
        res = yield self.client.get_count('test', CF)
        self.assert_(res == 2)
        
    @defer.inlineCallbacks
    def test_batch_mutate_and_remove(self):
        yield self.client.batch_mutate({'test': {CF: {COLUMN: 'test', COLUMN2: 'test2'}, SCF: { SCOLUMN: { COLUMN: 'test', COLUMN2: 'test2'} } }, 'test2': {CF: {COLUMN: 'test', COLUMN2: 'test2'}, SCF: { SCOLUMN: { COLUMN: 'test', COLUMN2: 'test2'} } } })
        res = yield self.client.get_slice('test', CF, names=(COLUMN, COLUMN2))
        self.assert_(res[0].column.value == 'test')
        self.assert_(res[1].column.value == 'test2')
        res = yield self.client.get_slice('test2', CF, names=(COLUMN, COLUMN2))
        self.assert_(res[0].column.value == 'test')
        self.assert_(res[1].column.value == 'test2')
        res = yield self.client.get_slice('test', SCF, names=(COLUMN, COLUMN2),
                                          super_column=SCOLUMN)
        self.assert_(res[0].column.value == 'test')
        self.assert_(res[1].column.value == 'test2')
        res = yield self.client.get_slice('test2', SCF, names=(COLUMN, COLUMN2),
                                          super_column=SCOLUMN)
        self.assert_(res[0].column.value == 'test')
        self.assert_(res[1].column.value == 'test2')
        yield self.client.batch_remove({CF: ['test', 'test2']}, names=['test', 'test2'])
        yield self.client.batch_remove({SCF: ['test', 'test2']}, names=['test', 'test2'], supercolumn=SCOLUMN)

    @defer.inlineCallbacks
    def test_batch_mutate_with_deletion(self):
        yield self.client.batch_mutate({'test': {CF: {COLUMN: 'test', COLUMN2: 'test2'}}})
        res = yield self.client.get_slice('test', CF, names=(COLUMN, COLUMN2))
        self.assert_(res[0].column.value == 'test')
        self.assert_(res[1].column.value == 'test2')
        yield self.client.batch_mutate({'test': {CF: {COLUMN: None, COLUMN2: 'test3'}}})
        res = yield self.client.get_slice('test', CF, names=(COLUMN, COLUMN2))
        self.assert_(len(res) == 1)
        self.assert_(res[0].column.value == 'test3')

    @defer.inlineCallbacks
    def test_multiget_slice_remove(self):
        yield self.client.insert('test', CF, 'testval', column=COLUMN)
        yield self.client.insert('test', CF, 'testval', column=COLUMN2)
        yield self.client.insert('test2', CF, 'testval2', column=COLUMN)
        res = yield self.client.multiget(['test', 'test2'], CF, column=COLUMN)
        self.assert_(res['test'][0].column.value == 'testval')
        self.assert_(res['test2'][0].column.value == 'testval2')
        res = yield self.client.multiget_slice(['test', 'test2'], CF)
        self.assert_(res['test'][0].column.value == 'testval')
        self.assert_(res['test'][1].column.value == 'testval')
        self.assert_(res['test2'][0].column.value == 'testval2')
        yield self.client.remove('test', CF, column=COLUMN)
        yield self.client.remove('test2', CF, column=COLUMN)
        res = yield self.client.multiget(['test', 'test2'], CF, column=COLUMN)
        self.assert_(len(res['test']) == 0)
        self.assert_(len(res['test2']) == 0)
        
    @defer.inlineCallbacks
    def test_range_slices(self):
        yield self.client.insert('test', CF, 'testval', column=COLUMN)
        yield self.client.insert('test', CF, 'testval', column=COLUMN2)
        yield self.client.insert('test2', CF, 'testval2', column=COLUMN)
        ks = yield self.client.get_range_slices(CF, start='', finish='')
        keys = [k.key for k in ks]
        for key in ['test', 'test2']:
            self.assert_(key in keys)

    @defer.inlineCallbacks
    def test_keyspace_manipulation(self):
        ksdef = KsDef(name=T_KEYSPACE, strategy_class='org.apache.cassandra.locator.SimpleStrategy', replication_factor=1, cf_defs=[])
        yield self.client.system_add_keyspace(ksdef)
        ks2 = yield self.client.describe_keyspace(T_KEYSPACE)
        self.assert_(ksdef == ks2, msg='%s != %s' % (ksdef, ks2))
        newname = T_KEYSPACE + '2'
        yield self.client.system_rename_keyspace(T_KEYSPACE, newname)
        ks2 = yield self.client.describe_keyspace(newname)
        ksdef.name = newname
        self.assert_(ksdef == ks2)
        yield self.client.system_drop_keyspace(newname)
        yield self.assertFailure(self.client.describe_keyspace(T_KEYSPACE), NotFoundException)
        yield self.assertFailure(self.client.describe_keyspace(newname), NotFoundException)

    @defer.inlineCallbacks
    def test_column_family_manipulation(self):
        cfdef = CfDef(KEYSPACE, T_CF, column_type='Standard', comparator_type='org.apache.cassandra.db.marshal.BytesType', comment='foo', min_compaction_threshold=5, gc_grace_seconds=86400, column_metadata=[], default_validation_class='org.apache.cassandra.db.marshal.BytesType', max_compaction_threshold=31)
        yield self.client.system_add_column_family(cfdef)
        ksdef = yield self.client.describe_keyspace(KEYSPACE)
        cfdef2 = [c for c in ksdef.cf_defs if c.name == T_CF][0]
        # we don't know the id ahead of time. copy the new one so the equality
        # comparison won't fail
        cfdef.id = cfdef2.id
        self.assert_(cfdef == cfdef2, msg='%s != %s' % (cfdef, cfdef2))
        newname = T_CF + '2'
        yield self.client.system_rename_column_family(T_CF, newname)
        ksdef = yield self.client.describe_keyspace(KEYSPACE)
        cfdef2 = [c for c in ksdef.cf_defs if c.name == newname][0]
        self.assert_(len([c for c in ksdef.cf_defs if c.name == T_CF]) == 0)
        cfdef.name = newname
        self.assert_(cfdef == cfdef2)
        yield self.client.system_drop_column_family(newname)
        ksdef = yield self.client.describe_keyspace(KEYSPACE)
        self.assert_(len([c for c in ksdef.cf_defs if c.name == newname]) == 0)

    @defer.inlineCallbacks
    def test_describes(self):
        name = yield self.client.describe_cluster_name()
        self.assert_(isinstance(name, str),
                     msg='cluster name object not str: %r' % name)
        self.assert_(len(name) > 0)
        partitioner = yield self.client.describe_partitioner()
        self.assert_(partitioner.startswith('org.apache.cassandra.'),
                     msg='partitioner is %r' % partitioner)
        snitch = yield self.client.describe_snitch()
        self.assert_(snitch.startswith('org.apache.cassandra.'),
                     msg='snitch is %r' % snitch)
        version = yield self.client.describe_version()
        self.assert_(isinstance(version, str), msg='version is %r' % version)
        self.assert_('.' in version, msg='version is %r' % version)
        schemavers = yield self.client.describe_schema_versions()
        self.assert_(isinstance(schemavers, dict),
                     msg='schema versions object is %r' % schemavers)
        self.assert_(len(schemavers) > 0)
        ring = yield self.client.describe_ring(KEYSPACE)
        self.assert_(isinstance(ring, list),
                     msg='ring description object is %r' % ring)
        self.assert_(len(ring) > 0)
        for r in ring:
            self.assert_(isinstance(r.start_token, str))
            self.assert_(isinstance(r.end_token, str))
            self.assert_(isinstance(r.endpoints, list))
            self.assert_(len(r.endpoints) > 0)
            for ep in r.endpoints:
                self.assert_(isinstance(ep, str))

    @defer.inlineCallbacks
    def test_errback(self):
        yield self.client.remove('poiqwe', CF)
        try:
            yield self.client.get('poiqwe', CF, column='foo')
        except Exception, e:
            pass
            
    @defer.inlineCallbacks
    def test_bad_params(self):
        for x in xrange(CONNS+1):
            try:
                # pass an int where a string is required
                yield self.client.get(12345, CF, column='foo')
            except Exception, e:
                pass
