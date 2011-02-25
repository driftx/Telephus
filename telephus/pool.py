# pool.py
#
# spread requests among connections to multiple nodes in a Cassandra cluster

import sys
import random
import socket
from time import time
from itertools import izip, groupby
from warnings import warn
from twisted.application import service
from twisted.internet import defer, protocol, error
from twisted.python import failure, log
from thrift import Thrift
from thrift.transport import TTwisted
from thrift.protocol import TBinaryProtocol
from telephus.protocol import (ManagedThriftRequest, APIMismatch, ClientBusy,
                               InvalidThriftRequest, match_thrift_version)
from telephus.cassandra import Cassandra, constants
from telephus.cassandra.ttypes import *

noop = lambda *a, **kw: None

class NoKeyspacesAvailable(UserWarning):
    """
    Indicates CassandraClusterPool could not collect information about the
    cluster ring, in order to automatically add nodes to the pool.

    When Cassandra's thrift interface allows specifying null for describe_ring
    (like the underlying java interface already does), we can remove this.
    """

class NoNodesAvailable(Exception):
    """
    Indicates there are nodes to which we are allowed to make another immediate
    connection. The argument to this exception should be the expected number
    of seconds before a node /will/ be available.
    """

def lame_log_insufficient_nodes(poolsize, pooltarget, pending_reqs, waittime):
    msg = '(No candidate nodes to expand pool to target size %d from %d;' \
          ' there are %d pending requests.' % (pooltarget, poolsize, pending_reqs)
    if waittime is None:
        msg += ')'
    else:
        msg += ' Expected candidate node available in %s seconds.)' % waittime
    log.msg(msg)

class CassandraPoolParticipantClient(TTwisted.ThriftClientProtocol):
    thriftFactory = TBinaryProtocol.TBinaryProtocolAcceleratedFactory

    def __init__(self):
        TTwisted.ThriftClientProtocol.__init__(self, Cassandra.Client,
                                               self.thriftFactory())

    def connectionMade(self):
        TTwisted.ThriftClientProtocol.connectionMade(self)
        self.factory.clientConnectionMade(self)

class CassandraPoolReconnectorFactory(protocol.ClientFactory):
    protocol = CassandraPoolParticipantClient
    connector = None
    my_proto = None
    last_error = None
    keep_working = True

    # store the keyspace this connection is set to. we will take thrift
    # requests along with the keyspace in which they expect to be made, and
    # change keyspaces if necessary. this is done this way to avoid having
    # another layer of queueing for requests in this class (in addition to the
    # queue in CassandraClusterPool), or special logic here to pass on
    # set_keyspace calls from the service at the right time (so already-queued
    # requests still get made in their right keyspaces).
    keyspace = None

    def __init__(self, node, service):
        self.node = node
        # if self.service is None, don't bother doing anything. nobody loves us.
        self.service = service
        self.my_proto = None
        self.pending_request = None

    def buildProtocol(self, addr):
        if self.service is not None:
            my_proto = protocol.ClientFactory.buildProtocol(self, addr)
            self.my_proto = my_proto
            return my_proto

    def clientConnectionMade(self, proto):
        if self.service is not None:
            self.service.client_conn_made(self)

    def clientConnectionFailed(self, connector, reason):
        self.my_proto = None
        if self.service is not None:
            self.connector = connector
            self.service.client_conn_failed(reason, self)

    def clientConnectionLost(self, connector, reason):
        p = self.my_proto
        self.my_proto = None
        if p is not None and self.service is not None:
            self.connector = connector
            self.service.client_conn_lost(self, reason)

    def stopFactory(self):
        # idempotent
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
        if p is not None and p.transport is not None:
            p.transport.loseConnection()

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
        """
        Retry this factory's connection. It is assumed that a previous
        connection was attempted and failed- either before or after a
        successful connection.
        """

        if self.connector is None:
            raise ValueError("No connector to retry")
        if self.service is None:
            return
        self.connector.connect()

    def prep_connection(self, creds=None, keyspace=None, check_ver=False):
        """
        Do login and set_keyspace tasks as necessary, and also check this
        node's idea of the Cassandra ring. Expects that our connection is
        alive.

        Return a Deferred that will fire with the ring information, or be
        errbacked if something goes wrong.
        """

        d = defer.succeed(0)
        if check_ver:
            d.addCallback(lambda _: self.my_describe_version())
            def gotVersion(ver):
                if not match_thrift_version(constants.VERSION, ver):
                    raise APIMismatch('%s remote is not compatible with %s telephus'
                                      % (ver, constants.VERSION))
                return True
            d.addCallback(gotVersion)
        if creds is not None:
            d.addCallback(lambda _: self.my_login(creds))
        if keyspace is not None:
            d.addCallback(lambda _: self.my_set_keyspace(keyspace))
        d.addCallback(lambda _: self.my_describe_ring(keyspace))
        return d

    # The following my_* methods are for internal use, to facilitate the
    # management of the pool and the queries we get. The user should make
    # use of the methods on CassandraClient.

    def my_login(self, creds):
        return self.execute(
            ManagedThriftRequest('login', AuthenticationRequest(credentials=creds))
        )

    def my_set_keyspace(self, keyspace):
        def store_keyspace(_):
            self.keyspace = keyspace
        d = self.execute(ManagedThriftRequest('set_keyspace', keyspace))
        d.addCallback(store_keyspace)
        return d

    def my_describe_ring(self, keyspace=None):
        if keyspace is None or keyspace == 'system':
            d = self.my_pick_non_system_keyspace()
        else:
            d = defer.succeed(keyspace)
        d.addCallback(lambda k: self.execute(ManagedThriftRequest('describe_ring', k)))
        def dummy(f):
            f.trap(NoKeyspacesAvailable)
            return ()
        d.addErrback(dummy)
        return d

    def my_describe_keyspaces(self):
        return self.execute(ManagedThriftRequest('describe_keyspaces'))

    def my_pick_non_system_keyspace(self):
        """
        Find a keyspace in the cluster which is not 'system', for the purpose
        of getting a valid ring view. Can't use 'system' or null.
        """
        d = self.my_describe_keyspaces()
        def pick_non_system(klist):
            for k in klist:
                if k.name != 'system':
                    return k.name
            err = NoKeyspacesAvailable("Can't gather information about the "
                                       "Cassandra ring; no non-system "
                                       "keyspaces available")
            warn(err)
            raise err
        d.addCallback(pick_non_system)
        return d

    def execute(self, req):
        method = getattr(self.my_proto.client, req.method, None)
        if method is None:
            raise InvalidThriftRequest("don't understand %s request" % req.method)

        d = defer.succeed(0)

        # get keyspace from the req
        keyspace = getattr(req, 'keyspace', None)
        if keyspace is not None and keyspace != self.keyspace:
            d.addCallback(lambda _: self.my_set_keyspace(keyspace))
        d.addCallback(lambda _: method(*req.args))
        return d

    def process_request_result(self, result, req, req_d, retries):
        self.pending_request = None
        if isinstance(result, failure.Failure):
            self.last_error = result
            if retries > 0 and self.service is not None:
                if result.check(*self.service.retryables):
                    self.service.resubmit(req, req_d, retries - 1)
                    return
            req_d.errback(result)
        else:
            req_d.callback(result)

    def work_on_request(self, reqtuple):
        self.queue_getter = None
        req, req_d, retries = reqtuple
        if self.pending_request is not None:
            raise ClientBusy('rejecting %s request' % req.method)
        self.pending_request = req
        d = self.execute(req)
        d.addBoth(self.process_request_result, req, req_d, retries)
        return d

    def maybe_do_more_work(self, _, q):
        if not self.keep_working:
            self.stopFactory()
        elif self.service is not None:
            self.service.reactor.callLater(0, self.work_on_queue, q)

    def scream_like_a_little_girl(self, fail):
        if self.service is not None:
            complain = self.service.err
        else:
            complain = log.err
        complain(fail, "Factory for connection to %s had problems dealing with"
                       " the queue" % (self.node,))
        # don't process more requests

    def work_on_queue(self, q):
        self.queue_getter = d = q.get()
        d.addCallback(self.work_on_request)
        d.addCallback(self.maybe_do_more_work, q)
        d.addErrback(lambda f: f.trap(CancelledError))
        d.addErrback(self.scream_like_a_little_girl)
        return d

    def finish_and_die(self):
        """
        If there is a request pending, let it finish and be handled, then
        disconnect and die. If not, cancel any pending queue requests and
        just die.
        """
        self.keep_working = False
        if self.queue_getter is not None:
            self.queue_getter.cancel()
        if self.pending_request is None:
            self.stopFactory()

class CassandraNode:
    """
    Represent a Cassandra node, in the same sense Cassandra uses.

    Keep track of connection success and failure history for some time, so
    that smarter decisions can be made about where to make new connections
    within a pool.

    Implement exponential backoff in reconnect time when connections fail.

    @ivar history_interval: Keep history entries for at least this many seconds

    @ivar max_delay: Forced delay between connection attempts will not exceed
        this value (although actual connection attempts may be farther apart
        than this, if the pool has enough connections without it)
    """

    history_interval = 86400
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

    def record_hist(self, value):
        now = time()
        if self.history and self.history[0][0] < (now - self.history_interval * 2):
            # it has been 2x history_interval; prune history
            cutoff = now - self.history_interval
            for n, (tstamp, hval) in enumerate(self.history):
                if tstamp > cutoff:
                    break
            self.history = self.history[n:]
        self.history.append((now, value))

    def conn_success(self):
        self.reconnect_delay = self.initial_delay
        self.can_reconnect_at = 0
        self.record_hist(None)

    def conn_fail(self, reason):
        now = time()
        self.record_hist(reason.value)
        newdelay = min(self.reconnect_delay * self.factor, self.max_delay)
        if self.jitter:
            newdelay = random.normalvariate(newdelay, newdelay * self.jitter)
        self.reconnect_delay = newdelay
        self.can_reconnect_at = now + newdelay

    def seconds_until_connect_ok(self):
        return self.can_reconnect_at - time()

    def __str__(self):
        return '<%s %s:%s>' % (self.__class__.__name__, self.host, self.port)

    __repr__ = __str__

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

    @ivar max_connections_per_node: do our best not not to exceed this many
        connections to a single Cassandra endpoint

    @ivar on_insufficient_nodes: if set to a callback, this will be called
        in the event that there are no valid places to connect to expand
        the pool to its target size. Regardless of actions taken by this
        callback, the service will wait until a node is expected to be
        available and then check again.

    @type on_insufficient_nodes: callback taking three arguments: the current
        size of the connection pool, the target size of the pool, and the
        number of seconds before a candidate node will be available to try
        connecting (or None, if no candidate is in sight).

    @ivar on_insufficient_conns: if set to a callback, this will be called
        when a request is made and all current connections are busy. The
        request will still be expected to go through, once another connection
        is available, but it may be helpful to know how often this is
        happening and possibly expand the pool. TODO: implement this

    @ivar request_retries: the default number of retries which will be
        performed for requests when the retry number is unspecified

    @ivar retryables: A list of Exception types which, if they are raised in
        the course of a Cassandra Thrift operation, mean both that (a) the
        request can be tried again on another connection, and that (b) if the
        connection was lost right after this error, it can be retried
        immediately
    """

    default_cassandra_thrift_port = 9160
    max_connections_per_node = 25
    on_insufficient_nodes = lame_log_insufficient_nodes
    on_insufficient_conns = noop
    request_retries = 0

    retryables = (IOError, socket.error, Thrift.TException,
                  TimedOutException, UnavailableException)

    def __init__(self, seed_list, keyspace=None, creds=None, thrift_port=None,
                 pool_size=None, conn_timeout=10, bind_address=None,
                 log_cb=log.msg, reactor=None):
        """
        Initialize a CassandraClusterPool.

        @param keyspace: If given and not None, determines the keyspace to
            which all connections in this pool will be made.

        @param creds: Credentials to use to authenticate Cassandra connections

        @type creds: A dict (or other mapping) of strings to strings

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
        self.keyspace = keyspace
        self.creds = creds
        self.request_queue = defer.DeferredQueue()

        if reactor is None:
            from twisted.internet import reactor
        self.reactor = reactor

        # A set of CassandraNode instances representing known nodes. This
        # includes nodes from the initial seed list, nodes seen in
        # describe_ring calls to existing nodes, and nodes explicitly added
        # by the addNode() method. Nodes are only removed from this set if
        # no connections have been successful in self.forget_node_interval
        # seconds, or by an explicit call to removeNode().
        self.nodes = set()

        # A set of CassandraPoolReconnectorFactory instances corresponding to
        # connections which are either live or pending. Failed attempts to
        # connect will remove a connector from this set. When connections are
        # lost, an immediate reconnect will be attempted.
        self.connectors = set()

        # A collection of objects from self.connectors corresponding to
        # existing, working (as far as we know) connections. This will be
        # derivable from self.connectors, but hopefully will be maintained to
        # present a good snapshot of what is alive, now, and what is not.
        # This is stored in a deque so that it can be efficiently rotated
        # to distribute requests.
        self.good_conns = set()

        # A set of CassandraPoolReconnectorFactory instances, formerly in
        # self.connectors, the connections for which are draining. No new
        # requests should be fed to these instances; they are tracked only so
        # that they can be terminated more fully in case this service is shut
        # down before they finish.
        self.dying_conns = set()

    def startService(self):
        service.Service.startService(self)
        for addr in self.seed_list:
            self.addNode((addr, self.thrift_port))
        self.fill_pool()

    def stopService(self):
        service.Service.stopService(self)
        for factory in self.connectors.copy():
            factory.stopFactory()
        self.connectors = set()
        self.good_conns = set()
        self.dying_conns = set()

    def addNode(self, node):
        if not isinstance(node, CassandraNode):
            node = CassandraNode(*node)
        if node in self.nodes:
            raise ValueError("%s is already known" % (node,))
        self.nodes.add(node)

    def removeNode(self, node):
        if not isinstance(node, CassandraNode):
            node = CassandraNode(*node)
        for f in self.all_connectors_to(node):
            self.remove_connector(f)
        for f in self.dying_conns.copy():
            if f.node == node:
                self.remove_connector(f)
        self.nodes.remove(n)

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
        return self.connectors.copy()

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
        return self.good_conns.copy()

    def num_active_conns(self):
        return len(self.good_conns)

    def all_active_conns_to(self, node):
        return [f for f in self.good_conns if f.node == node]

    def num_active_conns_to(self, node):
        return len(self.all_active_conns_to(node))

    def all_pending_conns(self):
        return self.connectors - self.good_conns

    def num_pending_conns(self):
        return len(self.all_pending_conns())

    def all_pending_conns_to(self, node):
        return [f for f in self.all_pending_conns() if f.node == node]

    def num_pending_conns_to(self, node):
        return len(self.all_pending_conns_to(node))


    def add_connection_score(self, node):
        """
        Return a numeric value that determines this node's score for adding
        a new connection. A negative value indicates that no connections
        should be made to this node for at least that number of seconds.
        A value of -inf indicates no connections should be made to this
        node for the foreseeable future.

        This score should ideally take into account the connectedness of
        available nodes, so that those with less current connections will
        get more.
        """

        conntime = node.seconds_until_connect_ok()
        if conntime > 0:
            return -conntime
        numconns = self.num_connectors_to(node)
        if numconns >= self.max_connections_per_node:
            return float('-Inf')
        return sys.maxint - numconns

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

    def update_known_nodes(self, ring):
        for tokenrange in ring:
            for addr in tokenrange.endpoints:
                node = CassandraNode(addr, self.thrift_port)
                if node not in self.nodes:
                    self.addNode(node)

    def choose_nodes_to_connect(self):
        while True:
            nodes = list(self.nodes)
            scores = map(self.add_connection_score, nodes)
            bestscore, bestnode = max(zip(scores, nodes))
            if bestscore < 0:
                raise NoNodesAvailable(-bestscore)
            yield bestnode

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
            nodes_and_counts = ((n, len(list(conns))) for (n, conns) in nodes_and_conns)
            bestnode, bestcount = max(nodes_and_counts, key=lambda (n,count): count)
            # should be safe from IndexError
            yield self.all_active_conns_to(bestnode)[0]

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
            self.dying_conns.add(f)

    def fill_pool(self):
        """
        Add connections as necessary to meet the target pool size. If there
        are no nodes to connect to (because we maxed out connections-per-node
        on all active connections and any unconnected nodes have pending
        reconnect timers), call the on_insufficient_nodes callback.
        """

        need = self.target_pool_size - self.num_connectors()
        if need <= 0:
            return
        try:
            for num, node in izip(xrange(need), self.choose_nodes_to_connect()):
                self.make_conn(node)
        except NoNodesAvailable, e:
            waittime = e.args[0]
            if self.on_insufficient_nodes:
                self.on_insufficient_nodes(self.num_connectors(), self.target_pool_size,
                                           waittime if waittime != float('Inf') else None)
            self.schedule_future_fill_pool(e.args[0])

    def schedule_future_fill_pool(self, seconds):
        if seconds == float('Inf'):
            return
        future_fill = getattr(self, 'future_fill_pool', None)
        if future_fill is None:
            self.future_fill_pool = self.reactor.callLater(seconds, self.fill_pool)
        else:
            future_fill.reset(seconds)

    def make_conn(self, node):
        self.log('Adding connection to %s' % (node,))
        f = CassandraPoolReconnectorFactory(node, self)
        self.reactor.connectTCP(node.host, node.port, f,
                                timeout=self.conn_timeout,
                                bindAddress=self.bind_address)
        self.connectors.add(f)

    def remove_good_conn(self, f):
        try:
            self.good_conns.remove(f)
        except KeyError:
            pass

    def remove_connector(self, f):
        f.stopFactory()
        self.remove_good_conn(f)
        try:
            self.connectors.remove(f)
        except KeyError:
            try:
                self.dying_conns.remove(f)
            except KeyError:
                pass

    def client_conn_failed(self, reason, f):
        self.err(reason, 'Thrift pool connection to %s failed' % (f.node,),
                 level=30)
        f.node.conn_fail(reason)
        self.remove_connector(f)
        self.fill_pool()

    def client_conn_made(self, f):
        f.my_proto.pool = self
        d = f.prep_connection(self.creds, self.keyspace)
        d.addCallback(self.client_ready, f)
        d.addErrback(self.client_conn_failed, f)

    def client_ready(self, ring, f):
        self.update_known_nodes(ring)
        f.node.conn_success()
        self.good_conns.add(f)
        self.log('Successfully added connection to %s to the pool' % (f.node,))
        f.work_on_queue(self.request_queue)

    def client_conn_lost(self, f, reason):
        self.err(reason, 'Thrift pool connection to %s was lost' % (f.node,),
                 level=30)
        if f.last_error is not None and f.last_error.check(*self.retryables):
            self.log('Retrying right away')
            self.remove_good_conn(f)
            f.retry()
        else:
            f.node.conn_fail(reason)
            self.remove_connector(f)
            self.fill_pool()

    def pushRequest(self, req, retries=None):
        retries = retries or self.request_retries
        req.keyspace = self.keyspace
        d = defer.Deferred()
        self.request_queue.put((req, d, retries))
        return d

    def resubmit(self, req, req_d, retries):
        # if DeferredQueue had a simple way to push requests back to the front
        # of the line, we could do that here. but no biggie. Just push.
        self.request_queue.put((req, req_d, retries))

    def set_keyspace(self, keyspace):
        self.keyspace = keyspace
