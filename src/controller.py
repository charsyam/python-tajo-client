import google.protobuf.service

class RpcController(google.protobuf.service.RpcController):
    def __init__(self):
        self.error_code = None
        self.error_message = None

    def handleError(self, error_code, error_message):
        self.error_code = error_code
        self.error_message = error_message

    def reset(self):
        self.error_code = None
        self.error_message = None

    def failed(self):
        return self.error_message is not None

    def error(self):
        return self.error_message
