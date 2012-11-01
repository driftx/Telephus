import struct

from thrift.transport.TTwisted import ThriftClientProtocol
from thrift.transport.TTransport import TTransportException

from twisted.internet import defer
from twisted.internet.protocol import connectionDone
from twisted.internet.threads import deferToThread

from puresasl.client import SASLClient

class ThriftSASLClientProtocol(ThriftClientProtocol):

    START = 1
    OK = 2
    BAD = 3
    ERROR = 4
    COMPLETE = 5

    MAX_LENGTH = 2 ** 31 - 1

    def __init__(self, client_class, iprot_factory, oprot_factory=None,
            host=None, service=None, mechanism='GSSAPI', **sasl_kwargs):
        ThriftClientProtocol.__init__(self, client_class, iprot_factory, oprot_factory)

        self._sasl_negotiation_deferred = None
        self._sasl_negotiation_status = None
        self.client = None

        if host is not None:
            self.createSASLClient(host, service, mechanism, **sasl_kwargs)

    def createSASLClient(self, host, service, mechanism, **kwargs):
        self.sasl = SASLClient(host, service, mechanism, **kwargs)

    def dispatch(self, msg):
        encoded = self.sasl.wrap(msg)
        len_and_encoded = ''.join((struct.pack('!i', len(encoded)), encoded))
        ThriftClientProtocol.dispatch(self, len_and_encoded)

    @defer.inlineCallbacks
    def connectionMade(self):
        self._sendSASLMessage(self.START, self.sasl.mechanism)
        initial_message = yield deferToThread(self.sasl.process)
        self._sendSASLMessage(self.OK, initial_message)

        while True:
            status, challenge = yield self._receiveSASLMessage()
            if status == self.OK:
                response = yield deferToThread(self.sasl.process, challenge)
                self._sendSASLMessage(self.OK, response)
            elif status == self.COMPLETE:
                if not self.sasl.complete:
                    msg = "The server erroneously indicated that SASL " \
                          "negotiation was complete"
                    raise TTransportException(msg, message=msg)
                else:
                    break
            else:
                msg = "Bad SASL negotiation status: %d (%s)" % (status, challenge)
                raise TTransportException(msg, message=msg)

        self._sasl_negotiation_deferred = None
        ThriftClientProtocol.connectionMade(self)

    def _sendSASLMessage(self, status, body):
        if body is None:
            body = ""
        header = struct.pack(">BI", status, len(body))
        self.transport.write(header + body)

    def _receiveSASLMessage(self):
        self._sasl_negotiation_deferred = defer.Deferred()
        self._sasl_negotiation_status = None
        return self._sasl_negotiation_deferred

    def connectionLost(self, reason=connectionDone):
        if self.client:
            ThriftClientProtocol.connectionLost(self, reason)

    def dataReceived(self, data):
        if self._sasl_negotiation_deferred:
            # we got a sasl challenge in the format (status, length, challenge)
            # save the status, let IntNStringReceiver piece the challenge data together
            self._sasl_negotiation_status, = struct.unpack("B", data[0])
            ThriftClientProtocol.dataReceived(self, data[1:])
        else:
            # normal frame, let IntNStringReceiver piece it together
            ThriftClientProtocol.dataReceived(self, data)

    def stringReceived(self, frame):
        if self._sasl_negotiation_deferred:
            # the frame is just a SASL challenge
            response = (self._sasl_negotiation_status, frame)
            self._sasl_negotiation_deferred.callback(response)
        else:
            # there's a second 4 byte length prefix inside the frame
            decoded_frame = self.sasl.unwrap(frame[4:])
            ThriftClientProtocol.stringReceived(self, decoded_frame)
