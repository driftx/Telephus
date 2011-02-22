# pool.py
#
# spread requests among connections to multiple nodes in a Cassandra cluster

import random
import socket
from time import time
from itertools import izip, chain
from twisted.application import service
from twisted.internet import defer, protocol
from thrift import Thrift
from telephus.cassandra import ttypes

noop = lambda *a, **kw: None

class CassandraPoolParticipantClient(protocol.Protocol):
    last_error = None

    def connectionMade(self):
        self.blah

class CassandraPoolReconnectorFactory(protocol.ClientFactory):
    protocol = CassandraPoolParticipantClient
    connector = None
    my_proto = None

    def __init__(self, node, service):
        self.node = node
        # if self.service is None, don't bother doing anything. nobody loves us.
        self.service = service
        self.my_proto = None

    def buildProtocol(self, addr):
        if self.service is not None:
            my_proto = protocol.ClientFactory.buildProtocol(self, addr)
            self.my_proto = my_proto
            self.service.client_conn_made(self, my_proto)
            return my_proto

    def clientConnectionFailed(self, connector, reason):
        self.my_proto = None
        if self.service is not None:
            self.connector = connector
            self.service.client_conn_failed(self, reason)

    def clientConnectionLost(self, connector, reason):
        p = self.my_proto
        self.my_proto = None
        if p is not None and self.service is not None:
            self.connector = connector
            self.service.client_conn_lost(self, p, reason)

    def stopFactory(self):
        protocol.ClientFactory.stopFactory(self)
        self.service = None
        if self.connector:
            try:
                self.connector.stopConnecting()
            except error.NotConnectingError:
                pass
        self.connector = None
        p = self.my_proto
        self.my_proto = None
        if self.my_proto is not None:
            self.my_proto.transport.loseConnection()

    def isConnecting(self):
        if self.connector is None:
            if self.my_proto is None:
                # initial connection attempt
                return True
            else:
                # initial connection succeeded and hasn't died
                return False
        return self.connector.state == 'connecting'

    def retry(self):
        if self.connector is None:
            raise ValueError("No connector to retry")
        if self.service is None:
            return
        self.connector.connect()

    def isActive(self):
        return self.my_proto is not None

class CassandraNode:
    # Maximum age of history entries to keep
    history_interval = 86400

    # Maximum delay between connection attempts
    max_delay = 600

    initial_delay = 0.5

    # NIST backoff factors
    factor = protocol.ReconnectingClientFactory.factor
    jitter = protocol.ReconnectingClientFactory.jitter

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.reconnect_delay = self.initial_delay
        self.can_reconnect_at = 0

        # a list of (timestamp, error) tuples, least recent first.
        # (timestamp, None) tuples will be inserted on a successful connection.
        self.history = []

    def conn_success(self):
        self.reconnect_delay = self.initial_delay
        self.can_reconnect_at = 0
        self.history.append((time(), None))

    def conn_fail(self, reason):
        now = time()
        self.history.append((now, reason.value))
        newdelay = min(self.reconnect_delay * self.factor, self.max_delay)
        if self.jitter:
            newdelay = random.normalvariate(newdelay, newdelay * self.jitter)
        self.reconnect_delay = newdelay
        self.can_reconnect_at = now + newdelay

    def __str__(self):
        return '<%s %s:%s>' % (self.__class__.__name__, self.host, self.port)

    def __eq__(self, other):
        return self.__class__ == other.__class__ \
           and self.host == other.host \
           and self.port == other.port

    def __hash__(self):
        return hash((self.__class__, self.host, self.port))

class CassandraClusterPool(service.Service):
    """
    Manage a pool of connections to nodes in a Cassandra cluster.

    Requests made to the pool will go to whichever host is the least loaded (as
    far as this class can tell). If the requests specify multiple retries, the
    retries will be executed on different hosts if possible.

    Will periodically check an unparticular connection to see if new nodes can
    be found, and add them to the pool.

    Note that like most Services, the pool will not start until startService is
    called. If you have a parent Service (like a
    L{twisted.service.application.Application} instance), set that to be this
    service's parent:

        >>> cluster.pool.setServiceParent(application)

    and the startService() and stopService() methods will be called when
    appropriate.

    @ivar default_cassandra_thrift_port: just what it says on the tin

    @ivar retryables: A list of Exception types which, if they lead to a
        connection being dropped, mean that the connection can be retried
        immediately
    """

    default_cassandra_thrift_port = 9160

    retryables = (IOError, socket.error, Thrift.TException,
                  ttypes.TimedOutException, ttypes.UnavailableException)

    def __init__(self, seed_list, thrift_port=None, pool_size=None,
                 conn_timeout=10, bind_address=None, log_cb=noop,
                 reactor=None):
        """
        Initialize a CassandraClusterPool.

        @param seed_list: An initial set of host addresses which, if
            connectable, are part of this cluster.

        @type seed_list: iterable

        @param thrift_port: The port to use for connections to Cassandra nodes

        @param pool_size: The target size for the connection pool. Naturally,
            the actual size may be higher or lower as nodes connect and
            disconnect, but an effort will be made to adjust toward this size.

        @type pool_size: int

        @param conn_timeout: The number of seconds before a pending connection
            is deemed unsuccessful and aborted. Of course, when a connection
            error can be detected before this time, the connection will be
            aborted appropriately.

        @type conn_timeout: float

        @param bind_address: The local interface to which to bind when making
            outbound connections. Default: determined by the system's socket
            layer.

        @type bind_address: str

        @param log_cb: A callable which is expected to work like
            L{twisted.python.log.msg}. Will be used when certain connection
            and disconnection events occur. The default is for these events
            not to be logged at all.

        @param reactor: The reactor instance to use when starting thrift
            connections or setting timers.
        """

        service.Service.__init__(self)
        self.seed_list = list(seed_list)
        if thrift_port is None:
            thrift_port = self.default_cassandra_thrift_port
        self.thrift_port = thrift_port
        if pool_size is None:
            pool_size = len(self.seed_list)
        self.target_pool_size = pool_size
        self.log = log_cb
        self.conn_timeout = conn_timeout
        self.bind_address = bind_address

        if reactor is None:
            from twisted.internet import reactor
        self.reactor = reactor
        self.connector = protocol.ClientCreator(reactor, CassandraPoolParticipantClient)

        # A list of CassandraNode instances representing known nodes. This
        # includes nodes from the initial seed list, nodes seen in
        # describe_ring calls to existing nodes, and nodes explicitly added
        # by the addNode() method. Nodes are only removed from this list if
        # no connections have been successful in self.forget_node_interval
        # seconds, or by an explicit call to removeNode().
        self.nodes = []

        # A list of CassandraPoolReconnectorFactory instances corresponding to
        # connections which are either live or pending. Failed attempts to
        # connect will remove a connector from this list. When connections are
        # lost, an immediate reconnect will be attempted.
        self.connectors = []

        # A collection of objects from self.connectors corresponding to
        # existing, working (as far as we know) connections. This will be
        # derivable from self.connectors, but hopefully will be maintained to
        # present a good snapshot of what is alive, now, and what is not.
        # This is stored in a deque so that it can be efficiently rotated
        # to distribute requests.
        self.good_conns = collections.deque()

        # A list of CassandraPoolReconnectorFactory instances, formerly in
        # self.connectors, the connections for which are draining. No new
        # requests should be fed to these instances; they are tracked only so
        # that they can be terminated more fully in case this service is shut
        # down before they finish.
        self.dying_conns = []

    def startService(self):
        service.Service.startService(self)
        self.fill_pool()

    def stopService(self):
        service.Service.stopService(self)
        for factory in self.connectors:
            factory.service = None
            factory.stopFactory()
        self.connectors = []
        self.good_conns = collections.deque()

    def addNode(self, node):
        if not isinstance(node, CassandraNode):
            node = CassandraNode(*node)
        if node in self.nodes:
            raise ValueError("%s is already known" % (node,))
        self.nodes.append(node)

    def removeNode(self, host, port):
        if not isinstance(node, CassandraNode):
            node = CassandraNode(*node)
        for f in self.all_connectors_to(node):
            self.remove_connector(f)
        for f in self.dying_conns[:]:
            if f.node == node:
                self.remove_connector(f)
        self.nodes = [n for n in self.nodes if n != node]

    def err(self, _stuff=None, _why=None, **kw):
        if _stuff is None:
            _stuff = failure.Failure()
        kw['isError'] = True
        kw['why'] = _why
        if isinstance(_stuff, failure.Failure):
            self.log(failure=_stuff, **kw)
        elif isinstance(_stuff, Exception):
            self.log(failure=failure.Failure(_stuff), **kw)
        else:
            self.log(repr(_stuff), **kw)


    # methods for inspecting current connection state

    def all_connectors(self):
        return self.connectors[:]

    def num_connectors(self):
        """
        Return the total number of current connectors, including both live and
        pending connections.
        """
        return len(self.connectors)

    def all_connectors_to(self, node):
        return [f for f in self.connectors if f.node == node]

    def num_connectors_to(self, host):
        return len(self.all_connectors_to(host))

    def all_active_conns(self):
        return self.good_conns[:]

    def num_active_conns(self):
        return len(self.good_conns)

    def all_active_conns_to(self, node):
        return [f for f in self.good_conns if f.node == node]

    def num_active_conns_to(self, node):
        return len(self.all_active_conns_to(node))

    def all_pending_conns(self):
        good_conn_set = set(self.good_conns)
        return [f for f in self.connectors if f.my_proto not in good_conn_set]

    def num_pending_conns(self):
        return len(self.all_pending_conns())

    def all_pending_conns_to(self, node):
        return [f for f in self.all_pending_conns() if f.node == node]

    def num_pending_conns_to(self, node):
        return len(self.all_pending_conns_to(node))


    def add_connection_score(self, node):
        # prefer to connect to least-redundantly-connected of known nodes,
        # but avoid nodes with recent problems.
        # XXX

    def adjust_pool_size(self, newsize):
        """
        Change the target pool size. If we have too many connections already,
        ask some to finish what they're doing and die (preferring to kill
        connections to the node that already has the most connections). If
        we have too few, create more.
        """

        if newsize < 0:
            raise ValueError("pool size must be nonnegative")
        self.target_pool_size = newsize
        self.kill_excess_pending_conns()
        self.kill_excess_conns()
        self.fill_pool()

    def choose_nodes_to_connect(self):
        while True:
            yield max(self.nodes, key=self.add_connection_score)

    def choose_pending_conns_to_kill(self):
        # prefer to junk pending conns to most-redundantly-connected node
        while True:
            pending_conns = self.all_pending_conns()
            if len(pending_conns) == 0:
                break
            yield max(pending_conns, key=lambda f: self.num_connectors_to(f.node))

    def choose_conns_to_kill(self):
        # prefer to junk conns to most-redundantly-connected node
        while True:
            active_conns = self.all_active_conns()
            if len(active_conns) == 0:
                break
            nodes_and_conns = groupby(active_conns, lambda f: f.node)
            nodes_and_counts = ((node, len(conns)) for (node, conns) in nodes_and_conns)
            node, count = max(nodes_and_counts, key=lambda (n,count): count)
            yield node

    def kill_excess_pending_conns(self):
        killnum = self.num_connectors() - self.target_pool_size
        if killnum <= 0:
            return
        for n, f in izip(xrange(killnum), self.choose_pending_conns_to_kill()):
            self.remove_connector(f)

    def kill_excess_conns(self):
        need_to_kill = self.num_active_conns() - self.target_pool_size
        if need_to_kill <= 0:
            return
        for n, f in izip(xrange(need_to_kill), self.choose_conns_to_kill()):
            f.finish_and_die()
            self.remove_connector(f)
            self.dying_conns.append(f)

    def fill_pool(self):
        need = self.target_pool_size - self.num_connectors()
        if need <= 0:
            return
        for n, c in izip(xrange(need), self.choose_nodes_to_connect()):
            f = CassandraPoolReconnectorFactory(c, self)
            self.reactor.connectTCP(c.host, c.port, f,
                                    timeout=self.conn_timeout,
                                    bindAddress=self.bind_address)
            self.connectors.append(f)

    def remove_good_conn(self, f):
        try:
            self.good_conns.remove(f)
        except ValueError:
            pass

    def remove_connector(self, f):
        f.stopFactory()
        self.remove_good_conn(f)
        try:
            self.connectors.remove(f)
        except ValueError:
            try:
                self.dying_conns.remove(f)
            except ValueError:
                pass

    def client_conn_failed(self, f, reason):
        self.err(reason, 'Thrift pool connection to %s failed' % (f.node,),
                 level=30)
        f.node.conn_fail(reason)
        self.remove_connector(f)
        self.fill_pool()

    def client_conn_made(self, f, proto):
        proto.pool = self
        f.node.conn_success()
        self.good_conns.append(f)
        self.log('Added connection to %s to the pool' % (f.node,))

    def client_conn_lost(self, f, proto, reason):
        self.err(reason, 'Thrift pool connection to %s was lost' % (f.node,),
                 level=30)
        if proto.last_error.check(*self.retryables):
            self.log('Retrying right away')
            self.remove_good_conn(f)
            f.retry()
        else:
            f.node.conn_fail(reason)
            self.remove_connector(f)
            self.fill_pool()
