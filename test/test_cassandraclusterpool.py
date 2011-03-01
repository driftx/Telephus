import sys
import pickle
import random
import contextlib
from functools import partial
from twisted.trial import unittest
from twisted.internet import address, defer, reactor, protocol
from twisted.application import service, internet
from twisted.protocols import basic
from telephus.pool import (CassandraClusterPool, CassandraPoolReconnectorFactory,
                           CassandraPoolParticipantClient)
from telephus.cassandra import Cassandra, constants
from telephus.cassandra.ttypes import *
from twisted.python import log

def deferwait(s):
    d = defer.Deferred()
    reactor.callLater(s, d.callback, None)
    return d

class CassandraClusterPoolTest(unittest.TestCase):
    def setUp(self):
        self.cass = FakeCassandraCluster(10)
        self.cass.startService()

    def tearDown(self):
        self.cass.stopService()
        del self.cass

    @contextlib.contextmanager
    def pool(self, *a, **kw):
        c = MockedConnectionCassandraClusterPool(self.cass, *a, **kw)
        c.startService()
        try:
            yield c
        finally:
            c.stopService()

    @defer.inlineCallbacks
    def test_set_keyspace(self):
        with self.pool(pool_size=10) as c:
            yield c.set_keyspace('keyspace_foo')
            yield c.get('key', 'cf')
            # should have changed two- not more or less
            self.assertEqual(sorted(n.in_keyspace for n in self.cass.nodes),
                             sorted([None] * 8 + ['keyspace_foo'] * 2))

    @defer.inlineCallbacks
    def test_bad_set_keyspace(self):
        with self.pool() as c:
            yield self.assertFailure(c.set_keyspace('i-dont-exist'),
                                     InvalidRequestException)
            yield c.get('key', 'cf')
            # should have changed none
            self.assertEqual([n.in_keyspace for n in self.cass.nodes],
                             [None] * 10)

    @defer.inlineCallbacks
    def test_ring_inspection(self):
        c = MockedConnectionCassandraClusterPool(self.cass, use_seeds=1, pool_size=5)
        self.assertEqual(len(c.seed_list), 1)
        c.startService()
        yield c.describe_cluster_name()
        self.assertEqual(sorted((n.host, n.port) for n in c.nodes),
                         sorted(('localhost', f.port) for f in self.cass.nodes))
        yield deferwait(1)
        self.assertEqual(c.num_active_conns(), 5)
        c.stopService()

    @defer.inlineCallbacks
    def test_storm(self):
        with self.pool() as c:
            for i in xrange(500):
                r = str(random.randint(1, 10000))
                ans = yield c.get('echo', r)
                self.assertEqual(r, ans)
            for n in self.cass.nodes:
                self.assertApproximates(50.0, n.reqs_handled, 10)

    @defer.inlineCallbacks
    def test_storm2(self):
        with self.pool() as c:
            for i in xrange(500):
                r = str(random.randint(1, 10000))
                wait = str(random.random() / 200)
                ans = yield c.get('echo_with_wait', r, wait)
                self.assertEqual(r, ans)
            for n in self.cass.nodes:
                self.assertApproximates(50.0, n.reqs_handled, 10)

    def test_retrying(self):
        pass

    def test_lower_pool_size(self):
        pass

    def test_raise_pool_size(self):
        pass

    def test_connection_leveling(self):
        pass

    def test_huge_pool(self):
        pass

    def test_problematic_conns(self):
        pass

    def test_manual_node_add(self):
        pass

    def test_manual_node_remove(self):
        pass

    def test_keyspace_connection(self):
        pass

    def test_conn_loss_during_idle(self):
        pass

    def test_conn_loss_during_request(self):
        pass

    def test_last_conn_loss_during_idle(self):
        pass

    def test_last_conn_loss_during_request(self):
        pass

class StringQueueProtocol(basic.Int16StringReceiver):
    def __init__(self):
        self.recvq = defer.DeferredQueue()

    def stringReceived(self, s):
        self.recvq.put(s)

    def readString(self):
        return self.recvq.get()

    def sendObj(self, obj):
        return self.sendString(pickle.dumps(obj))

    def readObj(self):
        return self.readString().addCallback(pickle.loads)

class FakeCassandraNodeProtocol(StringQueueProtocol):
    def connectionMade(self):
        StringQueueProtocol.connectionMade(self)
        log.msg("got connection at port %d" % self.factory.port)

    def stringReceived(self, s):
        self.factory.reqs_handled += 1
        methname, args = pickle.loads(s)
        try:
            meth = getattr(self, methname)
        except AttributeError:
            meth = partial(self.generic_response, methname)
        d = defer.maybeDeferred(meth, *args)
        d.addErrback(lambda f: f.value)
        d.addCallback(self.sendObj)

    def set_keyspace(self, ksname):
        if ksname not in [k.name for k in self.factory.keyspaces]:
            raise InvalidRequestException('no such keyspace: %s' % ksname)
        self.factory.in_keyspace = ksname

    def describe_keyspaces(self):
        return self.factory.keyspaces

    def describe_ring(self, keyspace):
        ring = self.factory.fakecluster.nodes
        return [TokenRange(0, 0, ['localhost:%d' % r.port]) for r in ring]

    def get(self, key, cf, *a):
        if key == 'echo':
            return cf.column_family
        elif key == 'echo_with_wait':
            d = deferwait(float(cf.column))
            d.addCallback(lambda _: cf.column_family)
            return d
        return None

    def generic_response(self, methname, *a):
        d = defer.Deferred()
        reactor.callLater(0.01, d.callback, None)
        return d

class FakeCassandraNodeFactory(protocol.ServerFactory):
    protocol = FakeCassandraNodeProtocol
    keyspaces = [
        KsDef('keyspace_foo',
              cf_defs=[
                  CfDef('keyspace_foo', 'cf_1', column_type='Standard'),
                  CfDef('keyspace_foo', 'scf_1', column_type='Super'),
              ]),
        KsDef('keyspace_bar')
    ]

    def __init__(self, fakecluster):
        self.fakecluster = fakecluster
        self.in_keyspace = None
        self.reqs_handled = 0

    def __repr__(self):
        return '<Fake cassandra node listening on %d>' % (self.port,)

class FakeCassandraCluster(service.MultiService):
    def __init__(self, num_nodes, start_port=41356):
        service.MultiService.__init__(self)
        self.nodes = []
        for x in xrange(num_nodes):
            f = FakeCassandraNodeFactory(self)
            port = start_port + x
            serv = internet.TCPServer(port, f)
            serv.setServiceParent(self)
            f.port = port
            self.nodes.append(f)

class MockedCassandraPoolParticipantClient(StringQueueProtocol):
    def __init__(self):
        StringQueueProtocol.__init__(self)
        self.iface = Cassandra.Iface
        self.client = self

    def connectionMade(self):
        StringQueueProtocol.connectionMade(self)
        self.factory.clientConnectionMade(self)

    def __getattr__(self, name):
        if name in self.iface:
            return self.make_faker(name)
        raise AttributeError(name)

    def make_faker(self, methodname):
        def do_cmd(*args):
            self.sendObj((methodname, args))
            return self.readObj().addCallback(self.raise_if_exc)
        return do_cmd

    def raise_if_exc(self, x):
        if isinstance(x, Exception):
            raise x
        return x

class MockedCassandraPoolReconnectorFactory(CassandraPoolReconnectorFactory):
    protocol = MockedCassandraPoolParticipantClient

class MockedConnectionCassandraClusterPool(CassandraClusterPool):
    conn_factory = MockedCassandraPoolReconnectorFactory

    def __init__(self, fakecluster, *a, **kw):
        seed_hosts = [('localhost', f.port) for f in fakecluster.nodes]
        use_seeds = kw.pop('use_seeds', len(seed_hosts))
        seed_hosts = seed_hosts[:use_seeds]
        CassandraClusterPool.__init__(self, seed_hosts, *a, **kw)
