from connection import TajoSessionConnection
from queryid import QueryId
from memoryresultset import TajoMemoryResultSet
from fetchresultset import TajoFetchResultSet

import proto.ClientProtos_pb2 as ClientProtos_pb2
import proto.CatalogProtos_pb2 as CatalogProtos_pb2
from proto.tajo_protos_pb2 import QueryState

import time

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
                                   response.resultSet.serializedTuples)

    def isQueryWaitingForSchedule(self, state):
        return state == QueryState.QUERY_NOT_ASSIGNED or \
                state == QueryState.QUERY_MASTER_INIT or \
                state == QueryState.QUERY_MASTER_LAUNCHED

    def isQueryInited(self, state):
        return state == QueryState.QUERY_NEW

    def isQueryRunning(self, state):
        return self.isQueryInited(state) or (state == QueryState.QUERY_RUNNING)

    def isQueryComplete(self, state):
        return self.isQueryWaitingForSchedule(state) == False and \
               self.isQueryRunning(state) == False

    def getQueryResultAndWait(self, queryId):
        if self.isNullQueryId(queryId.id):
            return self.createNullResultSet(queryId)

        status = self.getQueryStatus(queryId)
        while (status != None and self.isQueryComplete(status.state)):
            time.sleep(1)
            status = self.getQueryStatus(queryId)

        if status.state == QueryState.QUERY_SUCCEEDED:
            if status.hasResult():
                return self.getQueryResult(queryId);

        return self.createNullResultSet(queryId);

    def getQueryResult(self, queryId):
        if self.isNullQueryId(queryId):
            return self.createNullResultSet(queryId)

        response = getResultResponse(queryId)
        return TajoFetchResultSet(self, response.queryId, 5)

    def getResultResponse(self, queryId):
        request = ClientProtos_pb2.GetQueryResultRequest()
        request.sessionId.id = self.checkSessionAndGet()
        request.queryId.id = queryId.id
        request.queryId.seq = queryId.seq
        request.fetchRowNum = fetchRowNum
        return self.service.getQueryResult(request)
