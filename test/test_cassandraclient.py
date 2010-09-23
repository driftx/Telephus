from twisted.trial import unittest
from twisted.python.failure import Failure
from twisted.internet import defer, reactor
from telephus.protocol import ManagedCassandraClientFactory
from telephus.client import CassandraClient
from telephus.cassandra.ttypes import *

CONNS = 5

HOST = 'localhost'
PORT = 9160
KEYSPACE = 'Keyspace1'
CF = 'Standard1'
SCF = 'Super1'
COLUMN = 'foo'
COLUMN2 = 'foo2'
SCOLUMN = 'bar'

class CassandraClientTest(unittest.TestCase):
    def setUp(self):
        self.cmanager = ManagedCassandraClientFactory()
        self.client = CassandraClient(self.cmanager, KEYSPACE)
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
        yield self.client.insert('test', ColumnPath(CF, None, COLUMN), 'testval')
        yield self.client.insert('test2', CF, 'testval2', column=COLUMN)
        yield self.client.insert('test', ColumnPath(SCF, SCOLUMN, COLUMN), 'superval')
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
        self.assert_(res['test'].column.value == 'testval')
        self.assert_(res['test2'].column.value == 'testval2')
        res = yield self.client.multiget_slice(['test', 'test2'], CF)
        self.assert_(res['test'][0].column.value == 'testval')
        self.assert_(res['test'][1].column.value == 'testval')
        self.assert_(res['test2'][0].column.value == 'testval2')
        yield self.client.remove('test', CF, column=COLUMN)
        yield self.client.remove('test2', CF, column=COLUMN)
        res = yield self.client.multiget(['test', 'test2'], CF, column=COLUMN)
        self.assert_(res['test'].column == None)
        self.assert_(res['test2'].column == None)
        
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
    def test_errback(self):
        yield self.client.remove('poiqwe', CF)
        try:
            yield self.client.get('poiqwe', CF, column='foo')
        except Exception, e:
            pass
            
    @defer.inlineCallbacks
    def test_bad_params(self):
        try:
            # pass an int where a string is required
            yield self.client.get(12345, CF, column='foo')
        except Exception, e:
            pass
