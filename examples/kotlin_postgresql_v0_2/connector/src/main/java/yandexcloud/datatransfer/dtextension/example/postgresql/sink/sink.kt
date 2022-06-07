package yandexcloud.datatransfer.dtextension.example.postgresql.sink

import com.beust.klaxon.Klaxon
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import net.pwall.json.schema.JSONSchema
import yandexcloud.datatransfer.dtextension.example.postgresql.DtExtensionException
import yandexcloud.datatransfer.dtextension.example.postgresql.RspUtil
import yandexcloud.datatransfer.dtextension.v0_2.Common
import yandexcloud.datatransfer.dtextension.v0_2.Common.InitRsp
import yandexcloud.datatransfer.dtextension.v0_2.Data
import yandexcloud.datatransfer.dtextension.v0_2.Data.Column
import yandexcloud.datatransfer.dtextension.v0_2.Data.ColumnType
import yandexcloud.datatransfer.dtextension.v0_2.Data.ColumnValue
import yandexcloud.datatransfer.dtextension.v0_2.Data.DataChangeItem
import yandexcloud.datatransfer.dtextension.v0_2.Data.PlainRow
import yandexcloud.datatransfer.dtextension.v0_2.Data.Table
import yandexcloud.datatransfer.dtextension.v0_2.sink.SinkServiceGrpcKt
import yandexcloud.datatransfer.dtextension.v0_2.sink.SinkServiceOuterClass
import yandexcloud.datatransfer.dtextension.v0_2.sink.SinkServiceOuterClass.WriteRsp
import yandexcloud.datatransfer.dtextension.v0_2.sink.Write
import yandexcloud.datatransfer.dtextension.v0_2.sink.Write.WriteBeginSnapshotRsp
import yandexcloud.datatransfer.dtextension.v0_2.sink.Write.WriteControlItemRsp
import yandexcloud.datatransfer.dtextension.v0_2.sink.Write.WriteItemRsp
import yandexcloud.datatransfer.dtextension.v0_2.sink.Write.WriteDoneSnapshotRsp
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

enum class CleanupPolicy { DROP, TRUNCATE, DISABLED }

data class PostgresSinkParameters (
    val jdbc_conn_string: String,
    val user: String,
    val password: String,
    val transactionalSnapshot: Boolean = false,
    val transactionalStream: Boolean = false,
    val cleanupPolicy: String = "disabled",
) {
    fun getCleanupPolicy() : CleanupPolicy {
        return when (cleanupPolicy.lowercase()) {
            "drop" -> return CleanupPolicy.DROP
            "truncate" -> return CleanupPolicy.TRUNCATE
            "disabled" -> return CleanupPolicy.DISABLED
            else -> return CleanupPolicy.DISABLED
        }
    }
}

class PostgresSink : SinkServiceGrpcKt.SinkServiceCoroutineImplBase() {
    private val specificationPath = "/sink_spec.json";

    private fun ValidateSpec(jsonSpec: String) {
        val specPath = javaClass.getResource(specificationPath)
            ?: throw DtExtensionException("Spec file not found")
        val specSchema = JSONSchema.parse(specPath.readText())
        val output = specSchema.validateBasic(jsonSpec)
        if (output.errors != null) {
            val err = output.errors?.map { it.instanceLocation + "(" + it.keywordLocation + "):" + it.error }
                ?.joinToString(separator = "\n") ?: ""
            throw DtExtensionException(err)
        }
        // TODO add extra validation if needed
    }

    private fun connectToPostgreSQL(params: PostgresSinkParameters): Connection {
        return DriverManager.getConnection(params.jdbc_conn_string, params.user, params.password)
    }

    private fun toPostgreSQLType(colType : ColumnType): String {
        return when (colType) {
            ColumnType.COLUMN_TYPE_BOOL -> "boolean"
            ColumnType.COLUMN_TYPE_INT32 -> "int"
            ColumnType.COLUMN_TYPE_INT64 -> "bigint"
            ColumnType.COLUMN_TYPE_UINT32 -> "bigint"
            ColumnType.COLUMN_TYPE_UINT64 -> "bigint" // no data losses, but... data representation and semantics...
            ColumnType.COLUMN_TYPE_FLOAT -> "real"
            ColumnType.COLUMN_TYPE_DOUBLE -> "double precision"
            ColumnType.COLUMN_TYPE_JSON -> "json"
            ColumnType.COLUMN_TYPE_DECIMAL -> "numeric"
            ColumnType.COLUMN_TYPE_BIG_DECIMAL -> "numeric"
            ColumnType.COLUMN_TYPE_BIG_INTEGER -> "numeric"
            ColumnType.COLUMN_TYPE_UNIX_TIME -> "timestamp with time zone"
            ColumnType.COLUMN_TYPE_STRING -> "text"
            ColumnType.COLUMN_TYPE_BINARY -> "bytea"
            ColumnType.COLUMN_TYPE_UNSPECIFIED,
            ColumnType.UNRECOGNIZED -> throw DtExtensionException("Unknown type $colType")
        }
    }

    private fun generateSqlValue(column: Column, columnValue: ColumnValue): String {
        when (column.type) {
            ColumnType.COLUMN_TYPE_BOOL -> return if (columnValue.bool) {"TRUE"} else {"FALSE"}
            ColumnType.COLUMN_TYPE_INT32 -> return columnValue.int32.toString()
            ColumnType.COLUMN_TYPE_INT64 -> return columnValue.int64.toString()
            ColumnType.COLUMN_TYPE_UINT32 -> return columnValue.uint32.toString()
            ColumnType.COLUMN_TYPE_UINT64 -> return columnValue.uint64.toString()
            ColumnType.COLUMN_TYPE_FLOAT -> return columnValue.float.toString()
            ColumnType.COLUMN_TYPE_DOUBLE -> return columnValue.double.toString()
            ColumnType.COLUMN_TYPE_JSON -> return "'${columnValue.json}'"
            ColumnType.COLUMN_TYPE_DECIMAL -> return columnValue.decimal.asString
            ColumnType.COLUMN_TYPE_BIG_DECIMAL -> return columnValue.bigDecimal
            ColumnType.COLUMN_TYPE_BIG_INTEGER -> return columnValue.bigInteger
            ColumnType.COLUMN_TYPE_UNIX_TIME -> return "to_timestamp(${columnValue.unixTime}) :: timestamp AT TIME ZONE 'UTC'"
            ColumnType.COLUMN_TYPE_STRING -> return "'${columnValue.string}'"
            ColumnType.COLUMN_TYPE_BINARY -> return columnValue.binary.toStringUtf8()
            ColumnType.COLUMN_TYPE_UNSPECIFIED,
            ColumnType.UNRECOGNIZED,
            null -> throw DtExtensionException("Unknown type ${column.type}")
        }
    }

    private fun colSchemaToCreateTableLine(colSchema : Column): String {
        val name = colSchema.name
        val type = toPostgreSQLType(colSchema.type)
        val pkPostfix = if (colSchema.key) { "primary key" } else { "" }
        return "$name $type $pkPostfix"
    }

    private fun generateIdentity(columns: List<Data.Column>, plainRow: PlainRow): String {
        return columns.zip(plainRow.valuesList).filter { (col, _) -> col.key }.joinToString {
            (col, value) -> "${col.name} = ${generateSqlValue(col, value)}"
        }
    }

    private fun tableExists(conn: Connection, table: Table) : Boolean {
        val namespace = table.namespace.namespace
        val name = table.name
        val query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
                    WHERE  table_schema = 'public'
                    AND    table_name   = '"$namespace"."$name"'
            );""".trimIndent()
        conn.prepareStatement(query).use {
            val result = it.executeQuery()
            if (!result.next()) {
                return false
            }
            return true
        }
    }

    private fun createTable(conn: Connection, table: Table) {
        val namespace = table.namespace.namespace
        val name = table.name
        val fields = table.schema.columnsList.joinToString { colSchemaToCreateTableLine(it) }
        val query = "CREATE TABLE IF NOT EXISTS \"$namespace\".\"$name\" ($fields);"
        val stmt = conn.prepareStatement(query)
        stmt.execute()
    }

    private fun dropTable(conn: Connection, table: Table) {
        val namespace = table.namespace.namespace
        val name = table.name
        val query = "DROP TABLE IF EXISTS \"$namespace\".\"$name\";"
        val stmt = conn.prepareStatement(query)
        stmt.execute()
    }

    private fun truncateTable(conn: Connection, table: Table) {
        val namespace = table.namespace.namespace
        val name = table.name
        val query = "DELETE FROM \"$namespace\".\"$name\";"
        val stmt = conn.prepareStatement(query)
        stmt.execute()
    }

    private fun processDataChangeItem(batch: Statement, connection: Connection, dataChangeItem: DataChangeItem) {
        val table = dataChangeItem.table
        val opType = dataChangeItem.opType
        when (dataChangeItem.formatCase) {
            DataChangeItem.FormatCase.PLAIN_ROW ->
                when (opType) {
                    Data.OpType.OP_TYPE_INSERT ->
                        plainRowUpsert(batch, connection, table, dataChangeItem.plainRow)
                    Data.OpType.OP_TYPE_UPDATE ->
                        plainRowUpsert(batch, connection, table, dataChangeItem.plainRow)
                    Data.OpType.OP_TYPE_DELETE ->
                        plainRowDelete(batch, connection, table, dataChangeItem.plainRow)
                    Data.OpType.OP_TYPE_UNSPECIFIED,
                    Data.OpType.UNRECOGNIZED,
                    null -> throw DtExtensionException("unknown operation type: $opType")
                }
            DataChangeItem.FormatCase.PARQUET ->
                TODO("Need to implement parquet source first")
            null, DataChangeItem.FormatCase.FORMAT_NOT_SET ->
                throw DtExtensionException("unknown type of change item format, plain row or parquet are expected")
        }
    }

    private fun plainRowUpsert(batch: Statement, conn: Connection, table: Table, plainRow: PlainRow) {
        val namespace = table.namespace.namespace
        val name = table.name
        val fields = table.schema.columnsList.joinToString { it.name }
        val values = table.schema.columnsList.zip(plainRow.valuesList).joinToString {
                (column, value) -> generateSqlValue(column, value)
        }
        val keys = table.schema.columnsList.filter { it.key }.joinToString { it.name }
        val onConflict = if (!keys.isEmpty()) {
            """
                ON CONFLICT ($keys) DO UPDATE
                SET ($fields) =  ($values)
            """.trimIndent()
        } else ""
        val query = """
            INSERT INTO "$namespace"."$name" ($fields) VALUES ($values)
            $onConflict
            ;
            """.trimIndent()
        batch.addBatch(query)
    }

    private fun plainRowInsert(batch: Statement, conn: Connection, table: Table, plainRow: PlainRow) {
        val namespace = table.namespace.namespace
        val name = table.name
        val fields = table.schema.columnsList.joinToString { it.name }
        val values = table.schema.columnsList.zip(plainRow.valuesList).joinToString {
                (column, value) -> generateSqlValue(column, value)
        }
        val query = "INSERT INTO \"$namespace\".\"$name\" ($fields) VALUES ($values);"
        batch.addBatch(query)
    }

    private fun plainRowUpdate(batch: Statement, conn: Connection, table: Table, plainRow: PlainRow) {
        val namespace = table.namespace.namespace
        val name = table.name
        val fields = table.schema.columnsList.joinToString { it.name }
        val values = table.schema.columnsList.zip(plainRow.valuesList).joinToString {
                (column, value) -> generateSqlValue(column, value)
        }
        val identity = generateIdentity(table.schema.columnsList, plainRow)
        val query = "UPDATE \"$namespace\".\"$name\" SET ($fields) =  ($values) WHERE ($identity);"
        batch.addBatch(query)
    }
    private fun plainRowDelete(batch: Statement, conn: Connection, table: Table, plainRow: PlainRow) {
        val namespace = table.namespace.namespace
        val name = table.name
        val identity = generateIdentity(table.schema.columnsList, plainRow)
        val query = "DELETE FROM \"$namespace\".\"$name\" WHERE ($identity);"
        batch.addBatch(query)
    }

    override suspend fun spec(request: Common.SpecReq): Common.SpecRsp {
        val specPath = javaClass.getResource(specificationPath)
            ?: return Common.SpecRsp.newBuilder()
                .setResult(RspUtil.resultError("Spec file not found"))
                .build()
        return Common.SpecRsp.newBuilder()
            .setResult(RspUtil.resultOk)
            .setJsonSpec(specPath.readText())
            .build()
    }

    override suspend fun check(request: Common.CheckReq): Common.CheckRsp {
        try {
            this.ValidateSpec(request.jsonSettings)
        } catch (e: java.lang.Exception) {
            return Common.CheckRsp.newBuilder()
                .setResult(RspUtil.resultError("exception occured: ${e.message}"))
                .build()
        }
        return Common.CheckRsp.newBuilder()
            .setResult(RspUtil.resultOk)
            .build()
    }

    @OptIn(ExperimentalTime::class)
    override fun write(requests: Flow<SinkServiceOuterClass.WriteReq>): Flow<SinkServiceOuterClass.WriteRsp> {
        fun mkRsp(controlItem: Any): WriteRsp {
            val writeCtlRsp = WriteControlItemRsp.newBuilder()
            when (controlItem) {
                is InitRsp -> writeCtlRsp.initRsp = controlItem
                is WriteItemRsp -> writeCtlRsp.itemRsp = controlItem
                is WriteBeginSnapshotRsp -> writeCtlRsp.beginSnapshotRsp = controlItem
                is WriteDoneSnapshotRsp -> writeCtlRsp.doneSnapshotRsp = controlItem
                else -> throw IllegalArgumentException("Unknown control item type: ${controlItem.javaClass}")
            }
            return WriteRsp.newBuilder()
                .setResult(RspUtil.resultOk)
                .setControlItemRsp(writeCtlRsp)
                .build()
        }

        fun mkBadRsp(error: String): WriteRsp =
            WriteRsp.newBuilder().setResult(RspUtil.resultError(error)).build()
        return flow {
            // INFO: this is the only stateful resources per-gRPC call: connection to database and client ID
            // It is initialized when INIT_CONNECTION_REQ message with full endpoint specification comes
            var initConnection: Connection? = null
            // note, that you can use clientId to persist resources in database for particular client,
            // or if you write completely stateless connector, you may ignore clientId at all
            var clientId: String?
            // identifies if stream of change items should be transactional
            var transactionalStream = false
            // identifies cleanup policy
            var cleanupPolicy = CleanupPolicy.DISABLED
            var txStatement : Statement? = null

            requests.collect { req ->
                try {
                    when (req.controlItemReq?.controlItemReqCase) {
                        Write.WriteControlItemReq.ControlItemReqCase.INIT_REQ -> {
                            val initConnReq = req.controlItemReq.initReq
                            clientId = initConnReq.clientId ?: UUID.randomUUID().toString()
                            // check spec and initialize connection (as in previous handles)
                            this@PostgresSink.ValidateSpec(initConnReq.jsonSettings)
                            // parse params
                            val parameters = Klaxon().parse<PostgresSinkParameters>(initConnReq.jsonSettings)
                                ?: throw DtExtensionException("Parameters cannot be empty")
                            val newConnection = this@PostgresSink.connectToPostgreSQL(parameters)
                            initConnection?.close()
                            initConnection = newConnection
                            transactionalStream = parameters.transactionalStream

                            cleanupPolicy = parameters.getCleanupPolicy()
                            emit(mkRsp(InitRsp.newBuilder().setClientId(clientId).build()))
                        }
                        Write.WriteControlItemReq.ControlItemReqCase.BEGIN_SNAPSHOT_REQ -> {
                            val connection = initConnection ?: throw DtExtensionException("Connection not initiated")
                            val beginSnapshotReq = req.controlItemReq.beginSnapshotReq
                            val table = beginSnapshotReq.table
                            when (cleanupPolicy) {
                                CleanupPolicy.DROP -> {
                                    this@PostgresSink.dropTable(connection, table)
                                }
                                CleanupPolicy.TRUNCATE -> this@PostgresSink.truncateTable(connection, table)
                                else -> Unit // Do nothing
                            }
                            if (this@PostgresSink.tableExists(connection, table)) {
                                this@PostgresSink.createTable(connection, table)
                            }
                            val someState = "you may save here some state"
                            emit(mkRsp(WriteBeginSnapshotRsp.newBuilder().setSnapshotState(
                                ByteString.copyFromUtf8(someState)
                            ).build()))
                        }
                        Write.WriteControlItemReq.ControlItemReqCase.DONE_SNAPSHOT_REQ -> {
                            val doneSnapshotReq = req.controlItemReq.doneSnapshotReq
                            // do nothing special in this sink implementation
                            val restoredState = doneSnapshotReq.snapshotState.toStringUtf8()
                            println("Restored state on Done Snapshot: $restoredState")
                            emit(mkRsp(WriteDoneSnapshotRsp.getDefaultInstance()))
                        }
                        Write.WriteControlItemReq.ControlItemReqCase.ITEM_REQ -> {
                            val connection = initConnection ?: throw DtExtensionException("Connection not initiated")
                            val dataItemReq = req.controlItemReq.itemReq
                            when (dataItemReq.writeItemReqCase) {
                                Write.WriteItemReq.WriteItemReqCase.CHANGE_ITEM -> {
                                    if (transactionalStream) {
                                        if (connection.autoCommit) {
                                            // turn off auto commit to manually commit on checkpoint
                                            connection.autoCommit = false;
                                        }
                                    }
                                    if (txStatement == null) {
                                        txStatement = connection.createStatement()
                                    }
                                    // no response needed here, but we need to store change item in a database
                                    when (dataItemReq.changeItem.changeItemCase) {
                                        Data.ChangeItem.ChangeItemCase.DATA_CHANGE_ITEM ->  {
                                            processDataChangeItem(txStatement!!, connection, dataItemReq.changeItem.dataChangeItem)
                                        }
                                        Data.ChangeItem.ChangeItemCase.HOMO_CHANGE_ITEM -> {
                                            // skip this change items, because we do not generate homo change items on source
                                        }
                                        null, Data.ChangeItem.ChangeItemCase.CHANGEITEM_NOT_SET ->
                                            emit(mkBadRsp("unknown type of change item, data or homo change item are expected"))
                                    }
                                }
                                Write.WriteItemReq.WriteItemReqCase.CHECK_POINT -> {
                                    txStatement?.executeBatch()
                                    txStatement = null
                                    if (transactionalStream) {
                                        // this is a transaction border fo sequence of sent CHANGE_ITEM messages
                                        // reply with responce here to notify client that we are ready to accept next transaction
                                        connection.autoCommit = true // implicitly commits transaction
                                    }
                                    emit(mkRsp(WriteItemRsp.getDefaultInstance()))
                                }
                                null, Write.WriteItemReq.WriteItemReqCase.WRITEITEMREQ_NOT_SET ->
                                    emit(mkBadRsp("unknown type of write item request type, expected change item or checkpoint"))
                            }
                        }
                        null, Write.WriteControlItemReq.ControlItemReqCase.CONTROLITEMREQ_NOT_SET ->
                            emit(mkBadRsp("no control item request sent"))
                    }
                } catch (e: java.lang.Exception) {
                    emit(mkBadRsp("exception occured: ${e.message}; full: $e"))
                }
            }
            initConnection?.close()
        }
    }
}