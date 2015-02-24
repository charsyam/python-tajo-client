import socket
import google.protobuf.service
from controller import RpcController as Controller

class SocketFactory():
    @staticmethod
    def createSocket():
        return socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    @staticmethod
    def prepareHandle(host, port):
        s = SocketFactory.createSocket();
        try:
            s.connect((host, port))
        except socket.gaierror:
            s = None
        except socket.error:
            s = None

        return s

    @staticmethod
    def closeSocket(sock):
        if sock:
            try:
                sock.close()
            except:
                pass

        return


class RpcChannel(google.protobuf.service.RpcChannel):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.handle = SocketFactory.prepareHandle(self.host, self.port)

    def readMoreN(self, handle, size):
        count = 0
        data = ""
        while True:
            try:
                tmpData = handle.recv(4096)
                count += len(tmpData)
                data += str(tmpData)
                if count >= size:
                    return data
            except:
                raise Exception("Socket Read Error")

    def closeSocket(self, handle):
        SocketFactory.closeSocket(handle)
        self.handle = None

    def createController(self):
        return Controller()

    def prepareRequest(self, method, request):
        return None

    def sendData(self, handle, packets):
        try:
            for packet in packets:
                handle.sendall(packet)

        except socket.error:
            raise socket.error("packet send error")

    def sendRequest(self, rpc_request):
        return None

    def recevieResponse(self, handle):
        return None

    def parseResponse(self, rpc_response, response_class):
        return None

    def CallMethod(self, method, controller, request, response_class, done):
        if self.handle is None:
            raise Exception("Connection Error(%s:%s)"%(self.host, self.port))

        rpc_request = self.prepareRequest(method, request)
        packets = self.sendRequest(rpc_request)
        self.sendData(self.handle, packets)
        rpc_response = self.recevieResponse(self.handle)
        response = self.parseResponse(rpc_response, response_class)
        return response
