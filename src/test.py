import sys
sys.path.append("/Library/Python/2.7/site-packages")

import py.TajoMasterClientProtocol_pb2 as TajoMasterClientProtocol_pb2
import py.TajoIdProtos_pb2 as TajoIdProtos_pb2
import py.ClientProtos_pb2 as ClientProtos_pb2

from service import RpcService
from tajochannel import TajoRpcChannel

HOST="127.0.0.1"
PORT=26002

channel = TajoRpcChannel(HOST, PORT)
request = ClientProtos_pb2.CreateSessionRequest()
request.username = "charsyam"
request.baseDatabaseName = "default"

service = RpcService(TajoMasterClientProtocol_pb2.TajoMasterClientProtocolService_Stub,
                     channel)

response = service.createSession(request)

sessionId = response.sessionId
response = service.getAllDatabases(sessionId)
for value in response.values:
    print "Database: ", value

response = service.getCurrentDatabase(sessionId)
print response.value

#request = ClientProtos_pb2.QueryRequest()
#request.sessionId.id = sessionId.id
#request.query = "select 1"
#request.isJson = False
#response = service.submitQuery(request)

#print response
request = ClientProtos_pb2.GetQueryListRequest()
#request.sessionId.id = sessionId.id
response = service.getFinishedQueryList(request)
print response
