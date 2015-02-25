class RpcService(object):
    def __init__(self, service_stub_cls, channel):
        self.service_stub_cls = service_stub_cls
        self.channel = channel
        self.service = self.service_stub_cls(self.channel)

        for method in self.service_stub_cls.GetDescriptor().methods:
            func = lambda request, service=self, method=method.name, callback=None: \
                service.call(service_stub_cls.__dict__[method], request, callback)

            self.__dict__[method.name] = func

    def call(self, func, request, callback):
        controller = self.channel.createController()
        return func(self.service, controller, request, callback)
