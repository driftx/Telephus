from twisted.trial import unittest
import os

from twisted.internet import defer, reactor, error
from telephus.protocol import ManagedCassandraClientFactory
from telephus.client import CassandraClient

from telephus.cassandra import ttypes

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

# RF for SimpleStrategy keyspaces should be set on the 'replication_factor'
# attribute of KsDefs below this version
KS_RF_ATTRIBUTE = (19, 4, 0)

COUNTERS_SUPPORTED_API = (19, 10, 0)

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
        self.version = tuple(map(int, remote_ver.split('.')))

        self.my_keyspace = ttypes.KsDef(
            name=KEYSPACE,
            strategy_class='org.apache.cassandra.locator.SimpleStrategy',
            strategy_options={},
            cf_defs=[
                ttypes.CfDef(
                    keyspace=KEYSPACE,
                    name=CF,
                    column_type='Standard'
                ),
                ttypes.CfDef(
                    keyspace=KEYSPACE,
                    name=SCF,
                    column_type='Super'
                ),
                ttypes.CfDef(
                    keyspace=KEYSPACE,
                    name=IDX_CF,
                    column_type='Standard',
                    comparator_type='org.apache.cassandra.db.marshal.UTF8Type',
                    column_metadata=[
                        ttypes.ColumnDef(
                            name='col1',
                            validation_class='org.apache.cassandra.db.marshal.UTF8Type',
                            index_type=ttypes.IndexType.KEYS,
                            index_name='idxCol1')
                    ],
                    default_validation_class='org.apache.cassandra.db.marshal.BytesType'
                ),
            ]
        )

        if self.version <= KS_RF_ATTRIBUTE:
            self.my_keyspace.replication_factor = 1
        else:
            self.my_keyspace.strategy_options['replication_factor'] = '1'

        if self.version >= COUNTERS_SUPPORTED_API:
            self.my_keyspace.cf_defs.extend([
                ttypes.CfDef(
                    keyspace=KEYSPACE,
                    name=COUNTER_CF,
                    column_type='Standard',
                    default_validation_class='org.apache.cassandra.db.marshal.CounterColumnType'
                ),
                ttypes.CfDef(
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
        expressions = [ttypes.IndexExpression('col1', ttypes.IndexOperator.EQ, 'two')]
        res = yield self.client.get_indexed_slices(IDX_CF, expressions, start_key='')
        self.assertEquals(res[0].columns[0].column.value,'two')

    @defer.inlineCallbacks
    def test_counter_add(self):
        if self.version < COUNTERS_SUPPORTED_API:
            raise unittest.SkipTest('Counters are not supported before 0.8')

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
        if self.version < COUNTERS_SUPPORTED_API:
            raise unittest.SkipTest('Counters are not supported before 0.8')

        # test standard column counter
        yield self.client.add('test', COUNTER_CF, 1, column='col')
        res = yield self.client.get('test', COUNTER_CF, column='col')
        self.assertEquals(res.counter_column.value, 1)

        yield self.client.remove_counter('test', COUNTER_CF, column='col')
        yield self.assertFailure(self.client.get('test', COUNTER_CF, column='col'),
                                 ttypes.NotFoundException)

        # test super column counters
        yield self.client.add('test', SUPERCOUNTER_CF, 1, column='col', super_column='scol')
        res = yield self.client.get('test', SUPERCOUNTER_CF, column='col', super_column='scol')
        self.assertEquals(res.counter_column.value, 1)

        yield self.client.remove_counter('test', SUPERCOUNTER_CF,
                                         column='col', super_column='scol')
        yield self.assertFailure(self.client.get('test', SUPERCOUNTER_CF,
                                                 column='col', super_column='scol'),
                                 ttypes.NotFoundException)

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
        yield self.assertFailure(self.client.get('test_ttls', CF, column=COLUMN), ttypes.NotFoundException)

        yield self.client.batch_insert('test_ttls', CF, {COLUMN:'testval'}, ttl=1)
        res = yield self.client.get('test_ttls', CF, column=COLUMN)
        self.assertEqual(res.column.value, 'testval')
        yield self.sleep(2)
        yield self.assertFailure(self.client.get('test_ttls', CF, column=COLUMN), ttypes.NotFoundException)

        yield self.client.batch_mutate({'test_ttls': {CF: {COLUMN: 'testval'}}}, ttl=1)
        res = yield self.client.get('test_ttls', CF, column=COLUMN)
        self.assertEqual(res.column.value, 'testval')
        yield self.sleep(2)
        yield self.assertFailure(self.client.get('test_ttls', CF, column=COLUMN), ttypes.NotFoundException)

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
        except ttypes.InvalidRequestException:
            pass

        ksdef = ttypes.KsDef(name=T_KEYSPACE, strategy_class='org.apache.cassandra.locator.SimpleStrategy', strategy_options={}, cf_defs=[])
        if self.version <= KS_RF_ATTRIBUTE:
            ksdef.replication_factor = 1
        else:
            ksdef.strategy_options['replication_factor'] = '1'

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
        yield self.assertFailure(self.client.describe_keyspace(T_KEYSPACE), ttypes.NotFoundException)
        if DO_SYSTEM_RENAMING:
            yield self.assertFailure(self.client.describe_keyspace(ksdef.name), ttypes.NotFoundException)

    @defer.inlineCallbacks
    def test_column_family_manipulation(self):
        # CfDef attributes present in all supported c*/thrift-api versions
        common_attrs = (
            ('column_type', 'Standard'),
            ('comparator_type', 'org.apache.cassandra.db.marshal.BytesType'),
            ('comment', 'foo'),
            ('read_repair_chance', 1.0),
            ('column_metadata', []),
            ('gc_grace_seconds', 86400),
            ('default_validation_class', 'org.apache.cassandra.db.marshal.BytesType'),
            ('min_compaction_threshold', 5),
            ('max_compaction_threshold', 31),
        )
        cfdef = ttypes.CfDef(KEYSPACE, T_CF)
        for attr, val in common_attrs:
            setattr(cfdef, attr, val)

        yield self.client.system_add_column_family(cfdef)
        ksdef = yield self.client.describe_keyspace(KEYSPACE)
        cfdefs = [c for c in ksdef.cf_defs if c.name == T_CF]
        self.assertEqual(len(cfdefs), 1)
        cfdef2 = cfdefs[0]

        for attr, val in common_attrs:
            val1 = getattr(cfdef, attr)
            val2 = getattr(cfdef2, attr)
            self.assertEqual(val1, val2, 'attribute %s mismatch: %r != %r' % (attr, val1, val2))

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
        reactor.connectTCP('nonexistent.foobarexample.com', PORT, cmanager)
        yield self.failUnlessFailure(d, error.DNSLookupError, error.TimeoutError)
        cmanager.shutdown()
