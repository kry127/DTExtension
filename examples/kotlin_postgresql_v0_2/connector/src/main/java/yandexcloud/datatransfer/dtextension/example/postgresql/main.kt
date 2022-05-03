package yandexcloud.datatransfer.dtextension.example.postgresql

import io.grpc.BindableService
import io.grpc.ServerBuilder

fun main() {
    val port = 26926
    // start source service
    val server = ServerBuilder.forPort(port).addService(PostgreSQL() as BindableService).build()
    server.start()
}