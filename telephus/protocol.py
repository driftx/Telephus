from thrift.transport import TTwisted
from thrift.protocol import TBinaryProtocol
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet import defer, reactor
from twisted.internet.error import UserError
from twisted.python import failure
from telephus import translate
from telephus.cassandra.ttypes import *
from telephus.cassandra.c08 import Cassandra
from sys import exc_info

class ClientBusy(Exception):
    pass

class InvalidThriftRequest(Exception):
    pass

# Here for backwards compatibility
APIMismatch = translate.APIMismatch

class ManagedThriftRequest(object):
    def __init__(self, method, *args):
        self.method = method
        self.args = args

class ManagedThriftClientProtocol(TTwisted.ThriftClientProtocol):

    def __init__(self, iprot_factory, oprot_factory=None, keyspace=None, api_version=None):
        TTwisted.ThriftClientProtocol.__init__(self, Cassandra.Client, iprot_factory, oprot_factory)
        self.iprot_factory = iprot_factory
        self.deferred = None
        self.aborted = False
        self.keyspace = keyspace
        self.api_version = api_version

    def connectionMade(self):
        TTwisted.ThriftClientProtocol.connectionMade(self)
        self.client.protocol = self
        self.setupConnection() \
            .addCallbacks(self.setupComplete, self.setupFailed)

    def setupConnection(self):
        d = defer.succeed(True)
        if self.api_version == None:
            d.addCallback(lambda _: self.client.describe_version())
            d.addCallback(translate.getAPIVersion)
            def set_version(ver):
                self.api_version = ver
            d.addCallback(set_version)
        if self.keyspace:
            d.addCallback(lambda _: self.client.set_keyspace(self.keyspace))
        return d

    def setupComplete(self, res=None):
        self.factory.resetDelay()
        self.factory.clientIdle(self, res)

    def setupFailed(self, err):
        self.transport.loseConnection()
        self.factory.clientSetupFailed(err)

    def connectionLost(self, reason=None):
        if not self.aborted: # don't allow parent class to raise unhandled TTransport
                             # exceptions, the manager handled our failure
            TTwisted.ThriftClientProtocol.connectionLost(self, reason)
        self.factory.clientGone(self)

    def _complete(self, res=None):
        self.deferred = None
        self.factory.clientIdle(self)
        return res

    def submitRequest(self, request):
        if not self.deferred:
            fun = getattr(self.client, request.method, None)
            if not fun:
                raise InvalidThriftRequest
            else:
                args = translate.translateArgs(request, self.api_version)
                d = fun(*args)
                d.addCallback(lambda results: translate.postProcess(results, request.method))
            self.deferred = d
            d.addBoth(self._complete)
            return d
        else:
            raise ClientBusy

    def abort(self):
        self.aborted = True
        self.transport.loseConnection()

class AuthenticatedThriftClientProtocol(ManagedThriftClientProtocol):
    def __init__(self, keyspace, credentials, iprot_factory, oprot_factory=None, api_version=None):
        ManagedThriftClientProtocol.__init__(self, iprot_factory, oprot_factory,
                                             keyspace=keyspace, api_version=api_version)
        self.credentials = credentials

    def setupConnection(self):
        d = self.client.login(AuthenticationRequest(credentials=self.credentials))
        d.addCallback(lambda _: ManagedThriftClientProtocol.setupConnection(self))
        return d

class ManagedCassandraClientFactory(ReconnectingClientFactory):
    maxDelay = 45
    thriftFactory = TBinaryProtocol.TBinaryProtocolAcceleratedFactory
    protocol = ManagedThriftClientProtocol

    def __init__(self, keyspace=None, retries=0, credentials={}, api_version=None):
        self.deferred   = defer.Deferred()
        self.queue = defer.DeferredQueue()
        self.continueTrying = True
        self._protos = []
        self._pending = []
        self.request_retries = retries
        self.keyspace = keyspace
        self.credentials = credentials
        if credentials:
            self.protocol = AuthenticatedThriftClientProtocol
        self.api_version = api_version

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
                              self.thriftFactory(),
                              api_version=self.api_version)
        else:
            p = self.protocol(self.thriftFactory(),
                              keyspace=self.keyspace,
                              api_version=self.api_version)
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
                    AuthenticationRequest(credentials=credentials))))
        return defer.gatherResults(dfrds)

    def submitRequest(self, proto):
        def reqError(err, req, d, r):
            if err.check(InvalidRequestException, InvalidThriftRequest) or r < 1:
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
            if not d.called: d.errback(UserError(string="Shutdown requested"))
