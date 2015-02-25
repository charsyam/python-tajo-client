import sys

import tajo.proto.TajoMasterClientProtocol_pb2 as TajoMasterClientProtocol_pb2
import tajo.proto.TajoIdProtos_pb2 as TajoIdProtos_pb2
import tajo.proto.ClientProtos_pb2 as ClientProtos_pb2

from tajo.rpc.service import RpcService
from tajo.channel import TajoRpcChannel
from tajo.client import TajoClient

HOST="127.0.0.1"
PORT=26002

client = TajoClient(HOST, PORT, "tmpuser")
resultSet = client.executeQueryAndGetResult("select * from table1")
#resultSet = client.executeQueryAndGetResult("select 1")
while True:
    t = resultSet.nextTuple()
    if t is None:
        break

    print t
    resultSet.next()

