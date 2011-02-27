import sys
import pickle
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

class CassandraClusterPoolTest(unittest.TestCase):
    def setUp(self):
        self.cass = FakeCassandraCluster(10)
        self.cass.startService()

    def tearDown(self):
        self.cass.stopService()
        del self.cass

    @defer.inlineCallbacks
    def test_something(self): 
        c = MockedConnectionCassandraClusterPool(self.cass)
        c.startService()
        response = yield c.get('test_ks', 'cf', 'key')
        self.assertIdentical(response, None)
        c.stopService()

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
        methname, args = pickle.loads(s)
        try:
            meth = getattr(self, methname)
        except AttributeError:
            meth = partial(self.generic_response, methname)
        d = defer.maybeDeferred(meth, *args)
        d.addErrback(lambda f: f.value)
        d.addCallback(self.sendObj)

    def set_keyspace(self, ksname):
        self.factory.in_keyspace = ksname

    def describe_keyspaces(self):
        return self.factory.keyspaces

    def describe_ring(self, keyspace):
        ring = self.factory.fakecluster.nodes
        return [TokenRange(0, 0, [r]) for r in ring]

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
            return self.readObj()
        return do_cmd

class MockedCassandraPoolReconnectorFactory(CassandraPoolReconnectorFactory):
    protocol = MockedCassandraPoolParticipantClient

class MockedConnectionCassandraClusterPool(CassandraClusterPool):
    conn_factory = MockedCassandraPoolReconnectorFactory

    def __init__(self, fakecluster, *a, **kw):
        seed_hosts = [('localhost', f.port) for f in fakecluster.nodes]
        CassandraClusterPool.__init__(self, seed_hosts, *a, **kw)
