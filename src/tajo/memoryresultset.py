from rowdecoder import RowDecoder
from resultset import TajoResultSet

class TajoMemoryResultSet(TajoResultSet):
    def __init__(self, queryId, schema, serializedTuples):
        super(TajoMemoryResultSet, self).__init__()
        self.queryId = queryId
        self.schema = schema
        self.totalRow = len(serializedTuples)
        self.serializedTuples = serializedTuples
        self.decoder = RowDecoder(self.schema)
        self.cur = None

    def getQueryId(self):
        return self.queryId

    def nextTuple(self):
        if self.curRow < self.totalRow:
            self.cur = self.decoder.toTuple(self.serializedTuples[self.curRow])
            return self.cur

        return None
