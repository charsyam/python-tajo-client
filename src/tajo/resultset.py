class TajoResultSet(object):
    def __init__(self):
        self.curRow = 0
        self.cur = None
        self.schema = None
        self.totalRow = 0

    def getCurrentTuple(self):
        return self.cur

    def next(self):
        if self.totalRow <= 0:
            return False

        self.cur = self.nextTuple()
        self.curRow += 1
        if self.cur is not None:
            return True

        return False

    def nextTuple(self):
        raise Exception("Not Implemented")


