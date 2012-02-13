from twisted.trial import unittest
from twisted.python.failure import Failure
from twisted.internet import defer, reactor, error
from telephus.protocol import ManagedCassandraClientFactory, APIMismatch
from telephus.client import CassandraClient
from telephus import translate
from telephus.cassandra.ttypes import *
from telephus.translate import thrift_api_ver_to_cassandra_ver, CASSANDRA_08_VERSION
import os

CONNS = 5

HOST = os.environ.get('CASSANDRA_HOST', 'localhost')
PORT = 9160
KEYSPACE = 'TelephusTests'
T_KEYSPACE = 'TelephusTests2'
CF = 'Standard1'
SCF = 'Super1'
COUNTER_CF = 'Counter1'
SUPERCOUNTER_CF = 'SuperCounter1'
IDX_CF = 'IdxTestCF'
T_CF = 'TransientCF'
T_SCF = 'TransientSCF'
COLUMN = 'foo'
COLUMN2 = 'foo2'
SCOLUMN = 'bar'

# until Cassandra supports these again..
DO_SYSTEM_RENAMING = False

class CassandraClientTest(unittest.TestCase):
    @defer.inlineCallbacks
    def setUp(self):
        self.cmanager = ManagedCassandraClientFactory(keyspace='system')
        self.client = CassandraClient(self.cmanager)
        for i in xrange(CONNS):
            reactor.connectTCP(HOST, PORT, self.cmanager)
        yield self.cmanager.deferred

        remote_ver = yield self.client.describe_version()
        self.version = thrift_api_ver_to_cassandra_ver(remote_ver)

        self.my_keyspace = KsDef(
            name=KEYSPACE,
            strategy_class='org.apache.cassandra.locator.SimpleStrategy',
            strategy_options={'replication_factor': '1'},
            cf_defs=[
                CfDef(
                    keyspace=KEYSPACE,
                    name=CF,
                    column_type='Standard'
                ),
                CfDef(
                    keyspace=KEYSPACE,
                    name=SCF,
                    column_type='Super'
                ),
                CfDef(
                    keyspace=KEYSPACE,
                    name=IDX_CF,
                    column_type='Standard',
                    comparator_type='org.apache.cassandra.db.marshal.UTF8Type',
                    column_metadata=[
                        ColumnDef(
                            name='col1',
                            validation_class='org.apache.cassandra.db.marshal.UTF8Type',
                            index_type=IndexType.KEYS,
                            index_name='idxCol1')
                    ],
                    default_validation_class='org.apache.cassandra.db.marshal.BytesType'
                ),
            ]
        )
        if self.version == CASSANDRA_08_VERSION:
            self.my_keyspace.cf_defs.extend([
                CfDef(
                    keyspace=KEYSPACE,
                    name=COUNTER_CF,
                    column_type='Standard',
                    default_validation_class='org.apache.cassandra.db.marshal.CounterColumnType'
                ),
                CfDef(
                    keyspace=KEYSPACE,
                    name=SUPERCOUNTER_CF,
                    column_type='Super',
                    default_validation_class='org.apache.cassandra.db.marshal.CounterColumnType'
                ),
            ])

        yield self.client.system_add_keyspace(self.my_keyspace)
        yield self.client.set_keyspace(KEYSPACE)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.client.system_drop_keyspace(self.my_keyspace.name)
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
        self.assertEqual(res.column.value, 'testval')
        res = yield self.client.get('test2', CF, column=COLUMN)
        self.assertEqual(res.column.value, 'testval2')
        res = yield self.client.get('test', SCF, column=COLUMN, super_column=SCOLUMN)
        self.assertEqual(res.column.value, 'superval')
        res = yield self.client.get('test2', SCF, column=COLUMN, super_column=SCOLUMN)
        self.assertEqual(res.column.value, 'superval2')

    @defer.inlineCallbacks
    def test_batch_multikey_insert_get_slice_and_count(self):
        mapping = {'testA':{COLUMN: 'column1A', COLUMN2: 'column2A'}, 
                   'testB':{COLUMN: 'column1B', COLUMN2: 'column2B'}}
        yield self.client.batch_multikey_insert(CF, mapping)
        res = yield self.client.get_slice('testA', CF, names=(COLUMN, COLUMN2))
        self.assertEqual(res[0].column.value, 'column1A')
        self.assertEqual(res[1].column.value, 'column2A')        
        res = yield self.client.get_slice('testB', CF, names=(COLUMN, COLUMN2))
        self.assertEqual(res[0].column.value, 'column1B')
        self.assertEqual(res[1].column.value, 'column2B') 
                
    @defer.inlineCallbacks
    def test_batch_insert_get_slice_and_count(self):
        yield self.client.batch_insert('test', CF,
                                       {COLUMN: 'test', COLUMN2: 'test2'})
        yield self.client.batch_insert('test', SCF,
                               {SCOLUMN: {COLUMN: 'test', COLUMN2: 'test2'}})
        res = yield self.client.get_slice('test', CF, names=(COLUMN, COLUMN2))
        self.assertEqual(res[0].column.value, 'test')
        self.assertEqual(res[1].column.value, 'test2')
        res = yield self.client.get_slice('test', SCF, names=(COLUMN, COLUMN2),
                                          super_column=SCOLUMN)
        self.assertEqual(res[0].column.value, 'test')
        self.assertEqual(res[1].column.value, 'test2')
        res = yield self.client.get_count('test', CF)
        self.assertEqual(res, 2)

    @defer.inlineCallbacks
    def test_batch_mutate_and_remove(self):
        yield self.client.batch_mutate({'test': {CF: {COLUMN: 'test', COLUMN2: 'test2'}, SCF: { SCOLUMN: { COLUMN: 'test', COLUMN2: 'test2'} } }, 'test2': {CF: {COLUMN: 'test', COLUMN2: 'test2'}, SCF: { SCOLUMN: { COLUMN: 'test', COLUMN2: 'test2'} } } })
        res = yield self.client.get_slice('test', CF, names=(COLUMN, COLUMN2))
        self.assertEqual(res[0].column.value, 'test')
        self.assertEqual(res[1].column.value, 'test2')
        res = yield self.client.get_slice('test2', CF, names=(COLUMN, COLUMN2))
        self.assertEqual(res[0].column.value, 'test')
        self.assertEqual(res[1].column.value, 'test2')
        res = yield self.client.get_slice('test', SCF, names=(COLUMN, COLUMN2),
                                          super_column=SCOLUMN)
        self.assertEqual(res[0].column.value, 'test')
        self.assertEqual(res[1].column.value, 'test2')
        res = yield self.client.get_slice('test2', SCF, names=(COLUMN, COLUMN2),
                                          super_column=SCOLUMN)
        self.assertEqual(res[0].column.value, 'test')
        self.assertEqual(res[1].column.value, 'test2')
        yield self.client.batch_remove({CF: ['test', 'test2']}, names=['test', 'test2'])
        yield self.client.batch_remove({SCF: ['test', 'test2']}, names=['test', 'test2'], supercolumn=SCOLUMN)

    @defer.inlineCallbacks
    def test_batch_mutate_with_deletion(self):
        yield self.client.batch_mutate({'test': {CF: {COLUMN: 'test', COLUMN2: 'test2'}}})
        res = yield self.client.get_slice('test', CF, names=(COLUMN, COLUMN2))
        self.assertEqual(res[0].column.value, 'test')
        self.assertEqual(res[1].column.value, 'test2')
        yield self.client.batch_mutate({'test': {CF: {COLUMN: None, COLUMN2: 'test3'}}})
        res = yield self.client.get_slice('test', CF, names=(COLUMN, COLUMN2))
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].column.value, 'test3')

    @defer.inlineCallbacks
    def test_multiget_slice_remove(self):
        yield self.client.insert('test', CF, 'testval', column=COLUMN)
        yield self.client.insert('test', CF, 'testval', column=COLUMN2)
        yield self.client.insert('test2', CF, 'testval2', column=COLUMN)
        res = yield self.client.multiget(['test', 'test2'], CF, column=COLUMN)
        self.assertEqual(res['test'][0].column.value, 'testval')
        self.assertEqual(res['test2'][0].column.value, 'testval2')
        res = yield self.client.multiget_slice(['test', 'test2'], CF)
        self.assertEqual(res['test'][0].column.value, 'testval')
        self.assertEqual(res['test'][1].column.value, 'testval')
        self.assertEqual(res['test2'][0].column.value, 'testval2')
        yield self.client.remove('test', CF, column=COLUMN)
        yield self.client.remove('test2', CF, column=COLUMN)
        res = yield self.client.multiget(['test', 'test2'], CF, column=COLUMN)
        self.assertEqual(len(res['test']), 0)
        self.assertEqual(len(res['test2']), 0)

    @defer.inlineCallbacks
    def test_range_slices(self):
        yield self.client.insert('test', CF, 'testval', column=COLUMN)
        yield self.client.insert('test', CF, 'testval', column=COLUMN2)
        yield self.client.insert('test2', CF, 'testval2', column=COLUMN)
        ks = yield self.client.get_range_slices(CF, start='', finish='')
        keys = [k.key for k in ks]
        for key in ['test', 'test2']:
            self.assertIn(key, keys)

    @defer.inlineCallbacks
    def test_indexed_slices(self):
        yield self.client.insert('test1', IDX_CF, 'one', column='col1')
        yield self.client.insert('test2', IDX_CF, 'two', column='col1')
        yield self.client.insert('test3', IDX_CF, 'three', column='col1')
        expressions = [IndexExpression('col1', IndexOperator.EQ, 'two')]
        res = yield self.client.get_indexed_slices(IDX_CF, expressions, start_key='')
        self.assertEquals(res[0].columns[0].column.value,'two')

    @defer.inlineCallbacks
    def test_counter_batch_multikey_add(self):
        if self.version != CASSANDRA_08_VERSION:
            raise unittest.SkipTest('Counters are not supported in 0.7')
        mapping = {
            "keyA":{"col1A":1, "col2A":5},
            "keyB":{"col1B":2, "col2B":3}}
        yield self.client.batch_multikey_add(COUNTER_CF, mapping) 
        res = yield self.client.get('keyA', COUNTER_CF, column='col1A')
        self.assertEquals(res.counter_column.value, 1)        
        res = yield self.client.get('keyA', COUNTER_CF, column='col2A')
        self.assertEquals(res.counter_column.value, 5)    
        res = yield self.client.get('keyB', COUNTER_CF, column='col1B')
        self.assertEquals(res.counter_column.value, 2)        
        res = yield self.client.get('keyB', COUNTER_CF, column='col2B')
        self.assertEquals(res.counter_column.value, 3)    
        mapping = {
            "keyA":{"col1A":1, "col2A":1},
            "keyB":{"col1B":1, "col2B":1}}
        yield self.client.batch_multikey_add(COUNTER_CF, mapping) 
        res = yield self.client.get('keyA', COUNTER_CF, column='col1A')
        self.assertEquals(res.counter_column.value, 2)        
        res = yield self.client.get('keyA', COUNTER_CF, column='col2A')
        self.assertEquals(res.counter_column.value, 6)    
        res = yield self.client.get('keyB', COUNTER_CF, column='col1B')
        self.assertEquals(res.counter_column.value, 3)        
        res = yield self.client.get('keyB', COUNTER_CF, column='col2B')
        self.assertEquals(res.counter_column.value, 4)  
                
    @defer.inlineCallbacks
    def test_counter_batch_add(self):
        if self.version != CASSANDRA_08_VERSION:
            raise unittest.SkipTest('Counters are not supported in 0.7')
        mapping = {"col1":1, "col2":5}
        yield self.client.batch_add('test', COUNTER_CF, mapping) 
        res = yield self.client.get('test', COUNTER_CF, column='col1')
        self.assertEquals(res.counter_column.value, 1)        
        res = yield self.client.get('test', COUNTER_CF, column='col2')
        self.assertEquals(res.counter_column.value, 5)    
        mapping = {"col1":1, "col2":1}
        yield self.client.batch_add('test', COUNTER_CF, mapping) 
        res = yield self.client.get('test', COUNTER_CF, column='col1')
        self.assertEquals(res.counter_column.value, 2)        
        res = yield self.client.get('test', COUNTER_CF, column='col2')
        self.assertEquals(res.counter_column.value, 6)  

    @defer.inlineCallbacks
    def test_counter_add(self):
        if self.version != CASSANDRA_08_VERSION:
            raise unittest.SkipTest('Counters are not supported in 0.7')

        # test standard column counter
        yield self.client.add('test', COUNTER_CF, 1, column='col')
        res = yield self.client.get('test', COUNTER_CF, column='col')
        self.assertEquals(res.counter_column.value, 1)

        yield self.client.add('test', COUNTER_CF, 1, column='col')
        res = yield self.client.get('test', COUNTER_CF, column='col')
        self.assertEquals(res.counter_column.value, 2)

        # test super column counters
        yield self.client.add('test', SUPERCOUNTER_CF, 1, column='col', super_column='scol')
        res = yield self.client.get('test', SUPERCOUNTER_CF, column='col', super_column='scol')
        self.assertEquals(res.counter_column.value, 1)

        yield self.client.add('test', SUPERCOUNTER_CF, 1, column='col', super_column='scol')
        res = yield self.client.get('test', SUPERCOUNTER_CF, column='col', super_column='scol')
        self.assertEquals(res.counter_column.value, 2)

    @defer.inlineCallbacks
    def test_counter_remove(self):
        if self.version != CASSANDRA_08_VERSION:
            raise unittest.SkipTest('Counters are not supported in 0.7')

        # test standard column counter
        yield self.client.add('test', COUNTER_CF, 1, column='col')
        res = yield self.client.get('test', COUNTER_CF, column='col')
        self.assertEquals(res.counter_column.value, 1)

        yield self.client.remove_counter('test', COUNTER_CF, column='col')
        yield self.assertFailure(self.client.get('test', COUNTER_CF, column='col'),
                                 NotFoundException)

        # test super column counters
        yield self.client.add('test', SUPERCOUNTER_CF, 1, column='col', super_column='scol')
        res = yield self.client.get('test', SUPERCOUNTER_CF, column='col', super_column='scol')
        self.assertEquals(res.counter_column.value, 1)

        yield self.client.remove_counter('test', SUPERCOUNTER_CF,
                                         column='col', super_column='scol')
        yield self.assertFailure(self.client.get('test', SUPERCOUNTER_CF,
                                                 column='col', super_column='scol'),
                                 NotFoundException)

    def sleep(self, secs):
        d = defer.Deferred()
        reactor.callLater(secs, d.callback, None)
        return d

    @defer.inlineCallbacks
    def test_ttls(self):
        yield self.client.insert('test_ttls', CF, 'testval', column=COLUMN, ttl=1)
        res = yield self.client.get('test_ttls', CF, column=COLUMN)
        self.assertEqual(res.column.value, 'testval')
        yield self.sleep(2)
        yield self.assertFailure(self.client.get('test_ttls', CF, column=COLUMN), NotFoundException)

        yield self.client.batch_insert('test_ttls', CF, {COLUMN:'testval'}, ttl=1)
        res = yield self.client.get('test_ttls', CF, column=COLUMN)
        self.assertEqual(res.column.value, 'testval')
        yield self.sleep(2)
        yield self.assertFailure(self.client.get('test_ttls', CF, column=COLUMN), NotFoundException)

        yield self.client.batch_mutate({'test_ttls': {CF: {COLUMN: 'testval'}}}, ttl=1)
        res = yield self.client.get('test_ttls', CF, column=COLUMN)
        self.assertEqual(res.column.value, 'testval')
        yield self.sleep(2)
        yield self.assertFailure(self.client.get('test_ttls', CF, column=COLUMN), NotFoundException)

    def compare_keyspaces(self, ks1, ks2):
        self.assertEqual(ks1.name, ks2.name)
        self.assertEqual(ks1.strategy_class, ks2.strategy_class)
        self.assertEqual(ks1.cf_defs, ks2.cf_defs)

        def get_rf(ksdef):
            rf = ksdef.replication_factor
            if ksdef.strategy_options and \
               'replication_factor' in ksdef.strategy_options:
                rf = int(ksdef.strategy_options['replication_factor'])
            return rf

        def strat_opts_no_rf(ksdef):
            if not ksdef.strategy_options:
                return {}
            opts = ksdef.strategy_options.copy()
            if 'replication_factor' in ksdef.strategy_options:
                del opts['replication_factor']
            return opts

        self.assertEqual(get_rf(ks1), get_rf(ks2))
        self.assertEqual(strat_opts_no_rf(ks1), strat_opts_no_rf(ks2))

    @defer.inlineCallbacks
    def test_keyspace_manipulation(self):
        try:
            yield self.client.system_drop_keyspace(T_KEYSPACE)
        except InvalidRequestException:
            pass
        ksdef = KsDef(name=T_KEYSPACE, strategy_class='org.apache.cassandra.locator.SimpleStrategy', strategy_options={'replication_factor': '1'}, cf_defs=[])
        yield self.client.system_add_keyspace(ksdef)
        ks2 = yield self.client.describe_keyspace(T_KEYSPACE)
        self.compare_keyspaces(ksdef, ks2)

        if DO_SYSTEM_RENAMING:
            newname = T_KEYSPACE + '2'
            yield self.client.system_rename_keyspace(T_KEYSPACE, newname)
            ks2 = yield self.client.describe_keyspace(newname)
            ksdef.name = newname
            self.compare_keyspaces(ksdef, ks2)
        yield self.client.system_drop_keyspace(ksdef.name)
        yield self.assertFailure(self.client.describe_keyspace(T_KEYSPACE), NotFoundException)
        if DO_SYSTEM_RENAMING:
            yield self.assertFailure(self.client.describe_keyspace(ksdef.name), NotFoundException)

    @defer.inlineCallbacks
    def test_column_family_manipulation(self):
        cfdef = CfDef(KEYSPACE, T_CF,
            column_type='Standard',
            comparator_type='org.apache.cassandra.db.marshal.BytesType',
            comment='foo',
            row_cache_size=0.0,
            key_cache_size=200000.0,
            read_repair_chance=1.0,
            column_metadata=[],
            gc_grace_seconds=86400,
            default_validation_class='org.apache.cassandra.db.marshal.BytesType',
            key_validation_class='org.apache.cassandra.db.marshal.BytesType',
            min_compaction_threshold=5,
            max_compaction_threshold=31,
            row_cache_save_period_in_seconds=0,
            key_cache_save_period_in_seconds=3600,
            memtable_flush_after_mins=60,
            memtable_throughput_in_mb=249,
            memtable_operations_in_millions=1.1671875,
            replicate_on_write=False,
            merge_shards_chance=0.10000000000000001,
            row_cache_provider=None,
            key_alias=None,
        )
        post_07_fields = ['replicate_on_write', 'merge_shards_chance',
                          'key_validation_class', 'row_cache_provider', 'key_alias']
        post_08_fields = ['memtable_throughput_in_mb', 'memtable_flush_after_mins', 'memtable_operations_in_millions']
                         

        yield self.client.system_add_column_family(cfdef)
        ksdef = yield self.client.describe_keyspace(KEYSPACE)
        cfdef2 = [c for c in ksdef.cf_defs if c.name == T_CF][0]

        for field in post_07_fields:
            # Most of these are ignored in 0.7, so we can't reliably compare them
            setattr(cfdef, field, None)
            setattr(cfdef2, field, None)

        for field in post_08_fields:
            # These fields change from 0.8 to 1.0
            setattr(cfdef, field, None)
            setattr(cfdef2, field, None)

        # we don't know the id ahead of time. copy the new one so the equality
        # comparison won't fail
        cfdef.id = cfdef2.id
        self.assertEqual(cfdef, cfdef2)
        if DO_SYSTEM_RENAMING:
            newname = T_CF + '2'
            yield self.client.system_rename_column_family(T_CF, newname)
            ksdef = yield self.client.describe_keyspace(KEYSPACE)
            cfdef2 = [c for c in ksdef.cf_defs if c.name == newname][0]
            self.assertNotIn(T_CF, [c.name for c in ksdef.cf_defs])
            cfdef.name = newname
            self.assertEqual(cfdef, cfdef2)
        yield self.client.system_drop_column_family(cfdef.name)
        ksdef = yield self.client.describe_keyspace(KEYSPACE)
        self.assertNotIn(cfdef.name, [c.name for c in ksdef.cf_defs])

    @defer.inlineCallbacks
    def test_describes(self):
        name = yield self.client.describe_cluster_name()
        self.assertIsInstance(name, str)
        self.assertNotEqual(name, '')
        partitioner = yield self.client.describe_partitioner()
        self.assert_(partitioner.startswith('org.apache.cassandra.'),
                     msg='partitioner is %r' % partitioner)
        snitch = yield self.client.describe_snitch()
        self.assert_(snitch.startswith('org.apache.cassandra.'),
                     msg='snitch is %r' % snitch)
        version = yield self.client.describe_version()
        self.assertIsInstance(version, str)
        self.assertIn('.', version)
        schemavers = yield self.client.describe_schema_versions()
        self.assertIsInstance(schemavers, dict)
        self.assertNotEqual(schemavers, {})
        ring = yield self.client.describe_ring(KEYSPACE)
        self.assertIsInstance(ring, list)
        self.assertNotEqual(ring, [])
        for r in ring:
            self.assertIsInstance(r.start_token, str)
            self.assertIsInstance(r.end_token, str)
            self.assertIsInstance(r.endpoints, list)
            self.assertNotEqual(r.endpoints, [])
            for ep in r.endpoints:
                self.assertIsInstance(ep, str)

    @defer.inlineCallbacks
    def test_errback(self):
        yield self.client.remove('poiqwe', CF)
        try:
            yield self.client.get('poiqwe', CF, column='foo')
        except Exception, e:
            pass

    @defer.inlineCallbacks
    def test_bad_params(self):
        # This test seems to kill the thrift connection, so we're skipping it for now
        for x in xrange(CONNS+1):
            try:
                # pass an int where a string is required
                yield self.client.get(12345, CF, column='foo')
            except Exception, e:
                pass
    test_bad_params.skip = "Disabled pending further investigation..."

class ManagedCassandraClientFactoryTest(unittest.TestCase):
    @defer.inlineCallbacks
    def test_initial_connection_failure(self):
        cmanager = ManagedCassandraClientFactory()
        client = CassandraClient(cmanager)
        d = cmanager.deferred
        reactor.connectTCP('nonexistent.example.com', PORT, cmanager)
        yield self.assertFailure(d, error.DNSLookupError)
        cmanager.shutdown()

    @defer.inlineCallbacks
    def test_api_match(self):
        cmanager = ManagedCassandraClientFactory(require_api_version=None)
        client = CassandraClient(cmanager)
        d = cmanager.deferred
        conn = reactor.connectTCP(HOST, PORT, cmanager)
        yield d
        yield client.describe_schema_versions()
        api_ver = cmanager._protos[0].api_version
        yield cmanager.shutdown()

        # try with the right version explicitly required
        cmanager = ManagedCassandraClientFactory(require_api_version=api_ver)
        client = CassandraClient(cmanager)
        d = cmanager.deferred
        conn = reactor.connectTCP(HOST, PORT, cmanager)
        yield d
        yield client.describe_schema_versions()
        yield cmanager.shutdown()

        # try with a mismatching version explicitly required
        bad_ver = [v for (_, v) in translate.supported_versions if v != api_ver][0]
        cmanager = ManagedCassandraClientFactory(require_api_version=bad_ver)
        client = CassandraClient(cmanager)
        d = cmanager.deferred
        conn = reactor.connectTCP(HOST, PORT, cmanager)
        yield self.assertFailure(d, translate.APIMismatch)
        yield cmanager.shutdown()
