# pool.py
#
# spread requests among connections to multiple nodes in a Cassandra cluster

from time import time
from itertools import izip, chain
from twisted.application import service
from twisted.internet import defer, protocol

noop = lambda *a, **kw: None

class CassandraPoolParticipantClient(protocol.Protocol):
    def connectionMade(self):
        self.blah

    def connectionLost(self, reason):
        if self.pool is not None:
            self.pool.clientConnLost(reason, self, self.host)

class CassandraClusterPool(service.Service):
    """
    Manage a pool of connections to nodes in a Cassandra cluster.

    Requests made to the pool will go to whichever host is the least loaded (as
    far as this class can tell). If the requests specify multiple retries, the
    retries will be executed on different hosts if possible.

    Will periodically check an unparticular connection to see if new nodes can
    be found, and add them to the pool.

    Nodes to which the pool cannot successfully connect will be put in the "boo
    box" (a list of misbehaving or unavailable nodes) for some period of time.

    @ivar default_cassandra_thrift_port: just what it says on the tin
    """

    default_cassandra_thrift_port = 9160

    def __init__(self, seed_list, thrift_port=None, pool_size=None,
                 error_cb=noop, conn_timeout=10, bind_address=None,
                 log_cb=noop, reactor=None):

        service.Service.__init__(self)
        self.seed_list = list(seed_list)
        if thrift_port is None:
            thrift_port = self.default_cassandra_thrift_port
        self.thrift_port = thrift_port
        if pool_size is None:
            pool_size = len(self.seed_list)
        self.target_pool_size = pool_size
        self.error_cb = error_cb
        self.log = log_cb
        self.conn_timeout = conn_timeout
        self.bind_address = bind_address

        if reactor is None:
            from twisted.internet import reactor
        self.reactor = reactor
        self.connector = protocol.ClientCreator(reactor, CassandraPoolParticipantClient)

        # Maps host addresses to lists of existing, working (as far as we know)
        # connections, represented by CassandraPoolParticipantClient protocol
        # instances
        self.conns = {}

        # Maps host addresses to lists of cancellable Deferreds, indicating
        # that we are currently attempting to connect to that host
        self.pending_conns = {}

        # Maps host addresses to lists of existing but draining connections,
        # represented by CassandraPoolParticipantClient protocol instances.
        # No new queries should be submitted to these.
        self.dying_conns = {}

        # Maps host addresses to tuples of (timestamp, failure, IDelayedCall)
        # indicating that the host is on probation, and no attempts to connect
        # to it should be made right at the moment. The IDelayedCall instance
        # will return the host to the seed list eventually. Use
        # remove_from_boo_box() to put it back before that time.
        self.boo_box = {}

    def startService(self):
        service.Service.startService(self)
        self.fill_pool()

    def stopService(self):
        service.Service.stopService(self)
        for t, f, delayedcall in self.boo_box.items():
            if delayedcall.active():
                delayedcall.cancel()
        self.boo_box = {}
        for host, d in self.pending_conns.items():
            d.cancel()
        self.pending_conns = {}
        for host, proto in chain(self.dying_conns.items(), self.conns.items()):
            proto.pool = None
            proto.loseConnection()
        self.dying_conns = {}
        self.conns = {}

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

    def num_conns(self):
        return sum(len(clist) for clist in self.conns.itervalues())

    def num_pending_conns(self):
        return sum(len(clist) for clist in self.pending_conns.itervalues())

    def connectedness(self, host):
        return len(self.conns.get(host, ()))

    def adjust_pool_size(self, newsize):
        """
        Change the target pool size. If we have too many connections already,
        ask some to finish what they're doing and die (preferring to kill
        connections to the host that already has the most connections). If
        we have too few, create more.
        """

        if newsize < 0:
            raise ValueError("pool size must be nonnegative")
        self.target_pool_size = newsize
        self.kill_excess_conns()
        self.kill_excess_pending_conns()
        self.fill_pool()

    def choose_hosts_to_connect(self):
        # prefer to connect to least-redundantly-connected of known hosts.
        while True:
            yield min(self.seed_list, key=self.connectedness)

    def choose_conns_to_kill(self):
        # prefer to junk redundant conns to a single host
        while len(self.conns) > 0:
            host, connlist = max(self.conns.iteritems(), key=lambda (h,c): len(c))
            assert len(connlist) > 0, "self.conns has been mismanaged"
            yield host, connlist

    def choose_pending_conns_to_kill(self):
        # prefer to junk pending conns to most-redundantly-connected host
        while len(self.pending_conns) > 0:
            host, pendlist = max(self.pending_conns.iteritems(),
                                 key=lambda (h,p): self.connectedness(h))
            assert len(pendlist) > 0, "self.pending_conns has been mismanaged"
            yield host, pendlist

    def kill_excess_conns(self):
        num_conns = self.num_conns()
        need_to_kill = num_conns - newsize
        if need_to_kill <= 0:
            return
        for n, (host, clist) in izip(xrange(need_to_kill), self.choose_conns_to_kill()):
            conn = clist.pop()
            if len(clist) == 0:
                del self.conns[host]
            conn.finish_and_die()
            self.dying_conns.setdefault(host, []).append(conn)

    def kill_excess_pending_conns(self):
        num_conns = self.num_conns()
        num_pending = self.num_pending_conns()
        killnum = num_conns + num_pending - newsize
        if killnum <= 0:
            return
        for n, (host, pend) in izip(xrange(killnum), self.choose_pending_conns_to_kill()):
            pendconn = pend.pop()
            if len(pend) == 0:
                del self.pending_conns[host]
            pendconn.cancel()

    def fill_pool(self):
        need = self.target_pool_size - self.num_conns() - self.num_pending_conns()
        if need <= 0:
            return
        for n, host in izip(xrange(need), self.choose_hosts_to_connect()):
            if host in self.pending_conns:
                continue
            d = self.connector.connectTCP(host, self.thrift_port,
                                          timeout=self.conn_timeout,
                                          bindAddress=self.bind_address)
            d.addCallbacks(self.clientConnMade,
                           self.clientConnFailed,
                           callbackArgs=(host, d),
                           errbackArgs=(host, d))
            self.pending_conns.setdefault(host, []).append(d)

    def clientConnFailed(self, reason, host, d):
        self.err(reason, 'Thrift pool connection to %s failed' % (host,),
                 level=30)
        self.pending_conns.remove(connector.getDestination().host)
        self.put_in_boo_box(host, reason)
        self.fill_pool()

    def clientConnMade(self, proto, host, d):
        proto.pool = self
        proto.host = host
        try:
            pendlist = self.pending_conns[host]
        except KeyError:
            self.log('warning: conns to %s were not known to be pending' % host)
        else:
            try:
                pendlist.remove(d)
            except ValueError:
                self.log('warning: this conn to %s was not known to be pending' % host)
            if len(pendlist) == 0:
                del self.pending_conns[host]
        self.conns.setdefault(host, []).append(proto)

    def clientConnLost(self, reason, proto, host):
        self.err(reason, 'Thrift pool connection to %s was lost' % (host,),
                 level=30)
        # don't put in the boo box yet; as far as we know, the problem was
        # transitory or specifically requested (maybe the node was explicitly
        # removed from the pool). if the problem is ongoing, we'll see as soon
        # as we try to reconnect.
        self.clear_conn(host, proto)
        self.fill_pool()

    def put_in_boo_box(self, host, reason, penalty_time=None):
        if penalty_time is None:
            penalty_time = self.connection_failure_penalty_time
        try:
            self.seed_list.remove(host)
        except ValueError:
            pass
        try:
            oldtime, oldreason, delayedcall = self.boo_box[host]
        except KeyError:
            delayedcall = None
        if delayedcall is None or not delayedcall.active():
            delayedcall = self.reactor.callLater(penalty_time,
                                                 self.remove_from_boo_box, host)
        else:
            delayedcall.reset(penalty_time)
        self.boo_box[host] = (time(), reason, delayedcall)

    def remove_from_boo_box(self, host):
        _, _, delayedcall = self.boo_box.pop(host)
        if delayedcall.active():
            delayedcall.cancel()
        assert host not in self.seed_list, "boo box has been mismanaged"
        self.seed_list.append(host)
        self.fill_pool()
