package yandexcloud.datatransfer.dtextension.example.postgresql

import io.grpc.BindableService
import io.grpc.ServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService

fun main() {
    val port = 26926
    // start source service
    val server = ServerBuilder.forPort(port)
        .addService(PostgreSQL())
        .addService(ProtoReflectionService.newInstance())
        .build()
    server.start()
    server.awaitTermination()
}