import struct

from thrift.transport.TTwisted import TCallbackTransport
from thrift.transport import TTransport

from twisted.internet import defer
from twisted.protocols import basic
from twisted.internet.protocol import connectionDone, Protocol, _PauseableMixin

from puresasl.client import SASLClient

class ThriftSASLClientProtocol(Protocol, _PauseableMixin):

    START = 1
    OK = 2
    BAD = 3
    ERROR = 4
    COMPLETE = 5

    MAX_LENGTH = 2 ** 31 - 1

    def __init__(self, client_class, iprot_factory, oprot_factory=None,
            sasl_host=None, sasl_service=None, mechanism='GSSAPI', **sasl_kwargs):
        self._client_class = client_class
        self._iprot_factory = iprot_factory
        if oprot_factory is None:
            self._oprot_factory = iprot_factory
        else:
            self._oprot_factory = oprot_factory

        self.recv_map = {}
        self.started = defer.Deferred()

        self._startup_deferred = None

        self.sasl = SASLClient(sasl_host, sasl_service, mechanism,
                **sasl_kwargs)

    def dispatch(self, msg):
        encoded = self.sasl.wrap(msg)
        if len(encoded) >= self.MAX_LENGTH:
            raise basic.StringTooLongError(
                "Try to send %s bytes whereas maximum is %s" % (
                len(encoded), self.MAX_LENGTH))

        inner_str = struct.pack('!i', len(encoded)) + encoded
        self.transport.write(
            struct.pack('!i', len(inner_str)) + inner_str)

    @defer.inlineCallbacks
    def connectionMade(self):
        self._sendSASLMessage(self.START, self.sasl.mechanism)
        self._sendSASLMessage(self.OK, self.sasl.process())

        while True:
            status, challenge = yield self._receiveSASLMessage()
            if status == self.OK:
                self._sendSASLMessage(self.OK, self.sasl.process(challenge))
            elif status == self.COMPLETE:
                if not self.sasl.complete:
                    raise TTransport.TTransportException("The server erroneously indicated "
                            "that SASL negotiation was complete")
                else:
                    break
            else:
                raise TTransport.TTransportException("Bad SASL negotiation status: %d (%s)"
                        % (status, challenge))

        self._startup_deferred = None
        tmo = TCallbackTransport(self.dispatch)
        self.client = self._client_class(tmo, self._oprot_factory)
        self.started.callback(self.client)

    def _sendSASLMessage(self, status, body):
        header = struct.pack(">BI", status, len(body))
        self.transport.write(header + body)
        self.transport.flush()

    def _receiveSASLMessage(self):
        self._startup_deferred = defer.Deferred() \
                .addCallback(self._gotSASLMessage)
        return self._startup_deferred

    def _gotSASLMessage(self, data):
        header = self.transport.readAll(5)
        status, length = struct.unpack(">BI", header)
        if length > 0:
            payload = self.transport.readAll(length)
        else:
            payload = ""
        return status, payload

    def connectionLost(self, reason=connectionDone):
        for k, v in self.client._reqs.iteritems():
            tex = TTransport.TTransportException(
                type=TTransport.TTransportException.END_OF_FILE,
                message='Connection closed')
            v.errback(tex)

    def dataReceived(self, data):
        tr = TTransport.TMemoryBuffer(data)

        if self._startup_deferred:
            self._startup_deferred.callback(tr)
            return

        header = tr.readAll(8)[4:]  # the frame length is duped
        length, = struct.unpack('!i', header)
        encoded = tr.readAll(length)
        tr = TTransport.TMemoryBuffer(self.sasl.unwrap(encoded))

        iprot = self._iprot_factory.getProtocol(tr)
        (fname, mtype, rseqid) = iprot.readMessageBegin()

        try:
            method = self.recv_map[fname]
        except KeyError:
            method = getattr(self.client, 'recv_' + fname)
            self.recv_map[fname] = method

        method(iprot, mtype, rseqid)
