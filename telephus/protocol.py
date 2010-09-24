from thrift.transport import TTwisted
from thrift.protocol import TBinaryProtocol
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet import defer, reactor
from twisted.internet.error import UserError
from telephus.cassandra import Cassandra
from telephus.cassandra.ttypes import InvalidRequestException

class ClientBusy(Exception):
    pass

class InvalidThriftRequest(Exception):
    pass

class ManagedThriftRequest(object):
    def __init__(self, method, *args):
        self.method = method 
        self.args = args

class ManagedThriftClientProtocol(TTwisted.ThriftClientProtocol):
    def __init__(self, client_class, iprot_factory, oprot_factory=None):
        TTwisted.ThriftClientProtocol.__init__(self, client_class, iprot_factory, oprot_factory)
        self.deferred = None
        self.aborted = False
                
    def connectionMade(self):
        TTwisted.ThriftClientProtocol.connectionMade(self)
        self.client.protocol = self
        self.factory.clientIdle(self)
        
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
                d = fun(*request.args)
            self.deferred = d
            d.addBoth(self._complete)
            return d
        else:
            raise ClientBusy
        
    def abort(self):
        self.aborted = True
        self.transport.loseConnection()
        
class ManagedCassandraClientFactory(ReconnectingClientFactory):
    maxDelay = 5
    thriftFactory = TBinaryProtocol.TBinaryProtocolAcceleratedFactory
    protocol = ManagedThriftClientProtocol

    def __init__(self, retries=0):
        self.deferred   = defer.Deferred()
        self.queue = defer.DeferredQueue()
        self.continueTrying = True
        self._protos = []
        self._pending = []
        self.request_retries = retries

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

    def clientIdle(self, proto):
        if proto not in self._protos:
            self._protos.append(proto)
        self.submitRequest(proto)
        self._callback(True)

    def buildProtocol(self, addr):
        p = self.protocol(Cassandra.Client, self.thriftFactory())
        p.factory = self
        self.resetDelay()
        return p

    def clientGone(self, proto):
        self._protos.remove(proto)
            
    def submitRequest(self, proto):
        def reqError(err, req, d, r):
            if isinstance(err, InvalidRequestException) or \
               isinstance(err, InvalidThriftRequest) or r < 1:
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
                except Exception, e:
                    proto.abort()
                    d = defer.fail(e)
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
    
