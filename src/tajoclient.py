from service import RpcService
from tajochannel import TajoRpcChannel
from tajoconnection import TajoSessionConnection
from tajoqueryid import QueryId
from tajomemoryresultset import TajoMemoryResultSet
from tajofetchresultset import TajoFetchResultSet

import py.TajoMasterClientProtocol_pb2 as TajoMasterClientProtocol_pb2
import py.TajoIdProtos_pb2 as TajoIdProtos_pb2
import py.ClientProtos_pb2 as ClientProtos_pb2
import py.CatalogProtos_pb2 as CatalogProtos_pb2

OK_VALUE = 0
ERROR_VALUE = 1

class TajoClient(TajoSessionConnection, object):
    def __init__(self, host, port, username='tmpuser'):
        super(TajoClient, self).__init__(host, port, username)

    def executeQuery(self, sql):
        request = ClientProtos_pb2.QueryRequest()
        request.sessionId.id = self.checkSessionAndGet()
        request.query = sql
        request.isJson = False
        return self.service.submitQuery(request)

    def getQueryStatus(self, queryId):
        request = ClientProtos_pb2.GetQueryStatusRequest()
        request.queryId.id = queryId.id
        request.queryId.seq = queryId.seq
        return self.service.getQueryStatus(request)

    def createNullResultSet(self, queryId):
        return TajoMemoryResultSet(queryId, CatalogProtos_pb2.SchemaProto,
                             None, 0)

    def isNullQueryId(self, queryId):
        return queryId == QueryId.NULL_QUERY_ID

    def createResultSet(self, response, fetchRowNum):
        if response.HasField('tableDesc'):
            return TajoFetchResultSet(self, response.queryId, 5)
        else:
            return TajoMemoryResultSet(response.queryId, response.resultSet.schema,
                             response.resultSet.serializedTuples, fetchRowNum)

    def executeQueryAndGetResult(self, sql):
        response = self.executeQuery(sql)
        if response.resultCode == ERROR_VALUE:
            raise Exception(response.errorMessage)

        queryId = response.queryId
        if response.isForwarded is True:
            if queryId.id == QueryId.NULL_QUERY_ID:
                return self.createNullResultSet(queryId)
            else:
                return self.getQueryResultAndWait(queryId)

        if self.isNullQueryId(queryId.id) and response.maxRowNum == 0:
            return self.createNullResultSet(queryId)

        if response.HasField('resultSet') == False and response.HasField('tableDesc') == False:
            return self.createNullResultSet(queryId)

        return self.createResultSet(response, 10)

    def fetchNextQueryResult(self, queryId, fetchRowNum):
        request = ClientProtos_pb2.GetQueryResultDataRequest()
        request.sessionId.id = self.checkSessionAndGet()
        request.queryId.id = queryId.id
        request.queryId.seq = queryId.seq
        request.fetchRowNum = fetchRowNum
        response = self.service.getQueryResultData(request)

        return TajoMemoryResultSet(queryId, response.resultSet.schema,
                             response.resultSet.serializedTuples, fetchRowNum)

    def getQueryResultAndWait(self, queryId):
        if self.isNullQueryId(queryId.id):
            return self.createNullResultSet(queryId)

        queryStatus = self.getQueryStatus(queryId)

