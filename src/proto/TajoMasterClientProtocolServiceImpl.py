import TajoMasterClientProtocol_pb2
import ClientProtos_pb2

class TajoMasterClientProtocolServiceImpl(TajoMasterClientProtocol_pb2.TajoMasterClientProtocolService):
    def createSession(self, controller, request, done):
        print request
        response = ClientProtos_pb2.CreateSessionResponse()
        done.run(response)

