from concurrent import futures

import api.v0_2.source.source_service_pb2_grpc as src_grpc

import grpc

from source import S3Source


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    src_grpc.add_SourceServiceServicer_to_server(
        S3Source(), server)
    server.add_insecure_port('[::]:26926')
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
