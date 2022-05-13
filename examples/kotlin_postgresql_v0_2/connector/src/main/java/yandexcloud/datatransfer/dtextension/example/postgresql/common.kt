package yandexcloud.datatransfer.dtextension.example.postgresql

import yandexcloud.datatransfer.dtextension.v0_2.Common

const val connectorId = "kry127.postgresql_example"

// TODO make part of library?
object RspUtil {
    val resultOk: Common.Result = Common.Result.newBuilder().setOk(true).build()
    fun resultError(error: String): Common.Result {
        return Common.Result.newBuilder().setError(error).build()
    }
}

class DtExtensionException(error: String) : Exception(error)