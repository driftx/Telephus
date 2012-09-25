from sys import exc_info

from thrift.transport import TTwisted
from thrift.protocol import TBinaryProtocol
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet import defer
from twisted.internet.error import UserError
from twisted.python import failure

from telephus.cassandra import Cassandra, ttypes
from telephus._sasl import ThriftSASLClientProtocol

class ClientBusy(Exception):
    pass


class InvalidThriftRequest(Exception):
    pass


class ManagedThriftRequest(object):
    def __init__(self, method, *args):
        self.method = method
        self.args = args


class BaseThriftClientProtocol(object):

    _parent_protocol = None

    def __init__(self, iprot_factory, oprot_factory, keyspace):
        self.iprot_factory = iprot_factory
        self.deferred = None
        self.aborted = False
        self.keyspace = keyspace

    @defer.inlineCallbacks
    def connectionMade(self):
        try:
            yield defer.maybeDeferred(self._parent_protocol.connectionMade, self)
        except Exception:
            self.transport.loseConnection()
            raise

        yield self.started
        self.client.protocol = self
        self.setupConnection() \
            .addCallbacks(self.setupComplete, self.setupFailed)

    def connectionLost(self, reason=None):
        if not self.aborted: # don't allow parent class to raise unhandled TTransport
                             # exceptions, the manager handled our failure
            self._parent_protocol.connectionLost(self, reason)
        self.factory.clientGone(self)

    def setupConnection(self):
        if self.keyspace:
            return self.client.set_keyspace(self.keyspace)
        else:
            return defer.succeed(None)

    def setupComplete(self, res=None):
        self.factory.resetDelay()
        self.factory.clientIdle(self, res)

    def setupFailed(self, err):
        self.transport.loseConnection()
        self.factory.clientSetupFailed(err)

    def _complete(self, res=None):
        self.deferred = None
        self.factory.clientIdle(self)
        return res

    def submitRequest(self, request):
        if self.deferred:
            raise ClientBusy

        fun = getattr(self.client, request.method, None)
        if not fun:
            raise InvalidThriftRequest

        self.deferred = fun(*(request.args))
        return self.deferred.addBoth(self._complete)

    def abort(self):
        self.aborted = True
        self.transport.loseConnection()


class ManagedThriftClientProtocol(BaseThriftClientProtocol, TTwisted.ThriftClientProtocol):

    _parent_protocol = TTwisted.ThriftClientProtocol

    def __init__(self, iprot_factory, oprot_factory=None, keyspace=None):
        TTwisted.ThriftClientProtocol.__init__(self, Cassandra.Client, iprot_factory, oprot_factory)
        BaseThriftClientProtocol.__init__(self, iprot_factory, oprot_factory, keyspace)


class AuthenticatedThriftClientProtocol(ManagedThriftClientProtocol):

    def __init__(self, keyspace, credentials, iprot_factory, oprot_factory=None, **kwargs):
        ManagedThriftClientProtocol.__init__(self, iprot_factory, oprot_factory,
                                             keyspace=keyspace, **kwargs)
        self.credentials = credentials

    def setupConnection(self):
        auth = ttypes.AuthenticationRequest(credentials=self.credentials)
        d = self.client.login(auth)
        d.addCallback(lambda _: ManagedThriftClientProtocol.setupConnection(self))
        return d


class SASLThriftClientProtocol(BaseThriftClientProtocol, ThriftSASLClientProtocol):

    _parent_protocol = ThriftSASLClientProtocol

    def __init__(self, iprot_factory, oprot_factory=None, keyspace=None, **sasl_kwargs):
        ThriftSASLClientProtocol.__init__(self, Cassandra.Client,
                iprot_factory, oprot_factory, **sasl_kwargs)
        BaseThriftClientProtocol.__init__(self, iprot_factory, oprot_factory, keyspace)


class ManagedCassandraClientFactory(ReconnectingClientFactory):
    maxDelay = 45
    thriftFactory = TBinaryProtocol.TBinaryProtocolAcceleratedFactory
    protocol = ManagedThriftClientProtocol

    def __init__(self, keyspace=None, retries=0, credentials=None, sasl_kwargs=None):
        self.deferred = defer.Deferred()
        self.queue = defer.DeferredQueue()
        self.continueTrying = True
        self._protos = []
        self._pending = []
        self.request_retries = retries
        self.keyspace = keyspace
        self.credentials = credentials
        self.sasl_kwargs = sasl_kwargs
        if credentials:
            self.protocol = AuthenticatedThriftClientProtocol
        elif sasl_kwargs:
            self.protocol = SASLThriftClientProtocol

    def _errback(self, reason=None):
        if self.deferred:
            self.deferred.errback(reason)
            self.deferred = None

    def _callback(self, value=None):
        if self.deferred:
            self.deferred.callback(value)
            self.deferred = None

    def clientConnectionFailed(self, connector, reason):
        ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)
        self._errback(reason)

    def clientSetupFailed(self, reason):
        self._errback(reason)

    def clientIdle(self, proto, result=True):
        if proto not in self._protos:
            self._protos.append(proto)
        self.submitRequest(proto)
        self._callback(result)

    def buildProtocol(self, addr):
        if self.credentials:
            p = self.protocol(self.keyspace,
                              self.credentials,
                              self.thriftFactory())
        elif self.sasl_kwargs:
            p = self.protocol(self.thriftFactory(),
                              keyspace=self.keyspace,
                              **self.sasl_kwargs)
        else:
            p = self.protocol(self.thriftFactory(),
                              keyspace=self.keyspace)
        p.factory = self
        return p

    def clientGone(self, proto):
        try:
            self._protos.remove(proto)
        except ValueError:
            pass

    def set_keyspace(self, keyspace):
        """ switch all connections to another keyspace """
        self.keyspace = keyspace
        dfrds = []
        for p in self._protos:
            dfrds.append(p.submitRequest(ManagedThriftRequest('set_keyspace', keyspace)))
        return defer.gatherResults(dfrds)

    def login(self, credentials):
        """ authenticate all connections """
        dfrds = []
        for p in self._protos:
            dfrds.append(p.submitRequest(ManagedThriftRequest('login',
                    ttypes.AuthenticationRequest(credentials=credentials))))
        return defer.gatherResults(dfrds)

    def submitRequest(self, proto):
        def reqError(err, req, d, r):
            if err.check(ttypes.InvalidRequestException, InvalidThriftRequest) or r < 1:
                if err.tb is None:
                    try:
                        raise err.value
                    except Exception:
                        # make new Failure object explicitly, so that the same
                        # (traceback-less) one made by Thrift won't be retained
                        # and useful tracebacks thrown away
                        t, v, tb = exc_info()
                        err = failure.Failure(v, t, tb)
                d.errback(err)
                self._pending.remove(d)
            else:
                self.queue.put((req, d, r))

        def reqSuccess(res, d):
            d.callback(res)
            self._pending.remove(d)

        def _process((request, deferred, retries)):
            if not proto in self._protos:
                # may have disconnected while we were waiting for a request
                self.queue.put((request, deferred, retries))
            else:
                try:
                    d = proto.submitRequest(request)
                except Exception:
                    proto.abort()
                    d = defer.fail()
                retries -= 1
                d.addCallbacks(reqSuccess,
                               reqError,
                               callbackArgs=[deferred],
                               errbackArgs=[request, deferred, retries])

        return self.queue.get().addCallback(_process)

    def pushRequest(self, request, retries=None):
        retries = retries or self.request_retries
        d = defer.Deferred()
        self._pending.append(d)
        self.queue.put((request, d, retries))
        return d

    def shutdown(self):
        self.stopTrying()
        for p in self._protos:
            if p.transport:
                p.transport.loseConnection()
        for d in self._pending:
            if not d.called:
                d.errback(UserError(string="Shutdown requested"))
