import sys
sys.path.append("/Library/Python/2.7/site-packages")

import py.TajoMasterClientProtocol_pb2 as TajoMasterClientProtocol_pb2
import py.TajoIdProtos_pb2 as TajoIdProtos_pb2
import py.ClientProtos_pb2 as ClientProtos_pb2

from service import RpcService
from tajochannel import TajoRpcChannel
from tajoclient import TajoClient

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

