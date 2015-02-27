from resultset import TajoResultSet

class TajoFetchResultSet(TajoResultSet):
    def __init__(self, client, queryId, schema, fetchRowNum):
        super(TajoFetchResultSet, self).__init__()
        self.queryId = queryId
        self.fetchRowNum = fetchRowNum
        self.client = client
        self.finished = False
        self.resultSet = None
        self.schema = schema

    def isFinished(self):
        return self.finished

    def getQueryId(self):
        return self.queryId

    def nextTuple(self):
        if self.isFinished() is True:
            return None

        t = None
        if self.resultSet is not None:
            self.resultSet.next()
            t = self.resultSet.getCurrentTuple()

        if self.resultSet is None or t is None:
            self.resultSet = self.client.fetchNextQueryResult(self.queryId, self.schema, self.fetchRowNum)
            if self.resultSet is None:
                self.finished = True
                return None

            self.resultSet.next()
            t = self.resultSet.getCurrentTuple()

        if t is None:
            if self.resultSet is not None:
                self.resultSet = None

            self.finished = True

        return t
