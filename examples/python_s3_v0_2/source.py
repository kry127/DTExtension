import api.v0_2.source.source_service_pb2_grpc as src


class RouteGuideServicer(src.SourceServiceServicer):
    def Spec(self, request, context):
        return super().Spec(request, context)

    def Check(self, request, context):
        return super().Check(request, context)

    def Discover(self, request, context):
        return super().Discover(request, context)

    def Read(self, request_iterator, context):
        return super().Read(request_iterator, context)

    def Stream(self, request_iterator, context):
        raise NotImplementedError("Stream protocol is not applicable to this source")
