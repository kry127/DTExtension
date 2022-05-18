package yandexcloud.datatransfer.dtextension.example.postgresql.sink

import io.grpc.ServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService

fun main() {
    val port = 26927
    // start sink service
    val server = ServerBuilder.forPort(port)
        .addService(PostgresSink())
        .addService(ProtoReflectionService.newInstance())
        .build()

    Runtime.getRuntime().addShutdownHook(Thread {
        server.shutdown()
        server.awaitTermination()
    })

    server.start()
    server.awaitTermination()
}