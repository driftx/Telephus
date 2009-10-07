from thrift.transport import TTwisted
from thrift.protocol import TBinaryProtocol
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet import defer, reactor
from telephus.cassandra import Cassandra
import collections

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
                
    def connectionMade(self):
        TTwisted.ThriftClientProtocol.connectionMade(self)
        self.client.protocol = self
        self.factory.clientIdle(self)
        
    def connectionLost(self, reason=None):
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
        
class ManagedCassandraClientFactory(ReconnectingClientFactory):
    maxDelay = 5
    thriftFactory = TBinaryProtocol.TBinaryProtocolAcceleratedFactory
    protocol = ManagedThriftClientProtocol
    submitLoopSleep = 1

    def __init__(self):
        self.stack      = collections.deque()
        self.deferred   = defer.Deferred()
        self.continueTrying = True
        self.wanted_conns = []
        self.conns_out = []
        self._protos = []


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

    def getConnection(self):
        d = defer.Deferred()
        self.wanted_conns.append(d)
        return d

    def clientIdle(self, proto):
        if proto in self.conns_out:
            self.conns_out.remove(proto)
        self.startSubmit(proto)
        self._callback(True)

    def buildProtocol(self, addr):
        p = self.protocol(Cassandra.Client, self.thriftFactory())
        p.factory = self
        self._protos.append(p)
        reactor.callLater(0, self.startSubmit, p)
        self.resetDelay()
        return p

    def clientGone(self, proto):
        if proto in self.conns_out:
            self.conns_out.remove(proto)
        self._protos.remove(proto)
            
    def startSubmit(self, proto):
        if not proto.deferred is None:
            return
        if not proto.started.called:
            return
        if proto in self.conns_out:
            return
        if self.wanted_conns:
            d = self.wanted_conns.pop(0)
            self.conns_out.append(proto)
            d.callback(proto)
            return
        try:
            request, deferred = self.stack.popleft()
        except IndexError:
            reactor.callLater(self.submitLoopSleep, self.startSubmit, proto)
            return

        d = proto.submitRequest(request)
        d.addCallbacks(deferred.callback, deferred.errback)
        
    def pushRequest(self, request):
        d = defer.Deferred()
        self.stack.append((request, d))
        return d
    
    def shutdown(self):
        self.stopTrying()
        for p in self._protos:
            if p.transport:
                p.transport.loseConnection()
    