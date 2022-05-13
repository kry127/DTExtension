package yandexcloud.datatransfer.dtextension.example.postgresql

import io.grpc.ServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import yandexcloud.datatransfer.dtextension.example.postgresql.source.PostgresSource

fun main() {
    val port = 26926
    // start source service
    val server = ServerBuilder.forPort(port)
        .addService(PostgresSource())
        .addService(ProtoReflectionService.newInstance())
        .build()
    server.start()
    server.awaitTermination()
}