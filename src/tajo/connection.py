from rpc.service import RpcService
from channel import TajoRpcChannel

import proto.TajoMasterClientProtocol_pb2 as tajo_pb2
import proto.ClientProtos_pb2 as ClientProtos_pb2

class TajoSessionConnection():
    def __init__(self, host, port, username="tmpuser"):
        self.host = host
        self.port = port
        self.username = username
        self.sessionId = None
        self.channel = TajoRpcChannel(self.host, self.port)
        self.service = RpcService(tajo_pb2.TajoMasterClientProtocolService_Stub,
                                  self.channel)

    def createNewSessionId(self):
        request = ClientProtos_pb2.CreateSessionRequest()
        request.username = self.username
        response = self.service.createSession(request)
        self.sessionId = response.sessionId.id
        return self.sessionId

    def checkSessionAndGet(self):
        if self.sessionId is None:
            self.sessionId = self.createNewSessionId()

        return self.sessionId

