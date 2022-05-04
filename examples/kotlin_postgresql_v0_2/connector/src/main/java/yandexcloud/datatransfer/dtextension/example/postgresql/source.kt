package yandexcloud.datatransfer.dtextension.example.postgresql

import com.beust.klaxon.Klaxon
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import net.pwall.json.schema.JSONSchema
import org.postgresql.PGConnection
import org.postgresql.replication.LogSequenceNumber
import org.postgresql.replication.PGReplicationStream
import yandexcloud.datatransfer.dtextension.v0_2.ColumnCursorKt
import yandexcloud.datatransfer.dtextension.v0_2.Common
import yandexcloud.datatransfer.dtextension.v0_2.Common.ColumnCursor
import yandexcloud.datatransfer.dtextension.v0_2.Common.Cursor
import yandexcloud.datatransfer.dtextension.v0_2.Data.*
import yandexcloud.datatransfer.dtextension.v0_2.source.Control.*
import yandexcloud.datatransfer.dtextension.v0_2.source.Control.StreamChangeRsp.EndOfStream
import yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceGrpcKt
import yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceOuterClass
import yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceOuterClass.ReadRsp
import yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceOuterClass.StreamRsp
import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.util.*


const val connectorId = "kry127.postgresql_example"

// TODO make part of library?
object RspUtil {
    val resultOk: Common.Result = Common.Result.newBuilder().setOk(true).build()
    fun resultError(error: String): Common.Result {
        return Common.Result.newBuilder().setError(error).build()
    }
}

class DtExtensionException(error: String) : Exception(error)

object PsqlQueries {
    val pgSystemSchemas = listOf("pg_catalog", "information_schema")
    val pgSystemTableNames = listOf("repl_mon", "pg_stat_statements")

    fun parsePgType(pgType: String): ColumnType {
        val pgPrefix = "pg:"
        val pgTypeTrim = if (pgType.startsWith(pgPrefix)) {
            pgType.substring(pgPrefix.length)
        } else pgType

        if (pgTypeTrim.startsWith("character") or pgTypeTrim.startsWith("character varying")) {
            return ColumnType.COLUMN_TYPE_STRING
        }
        if (pgTypeTrim.startsWith("bit(") or pgTypeTrim.startsWith("bit varying(")) {
            return ColumnType.COLUMN_TYPE_BINARY
        }
        when (pgType) {
            "timestamp without time zone", "timestamp with time zone", "time without time zone", "time with time zone", "date" ->
                return ColumnType.COLUMN_TYPE_ISO_TIME
            "uuid", "name", "text", "interval", "char", "abstime", "money"
            -> return ColumnType.COLUMN_TYPE_STRING
            "boolean" -> return ColumnType.COLUMN_TYPE_BOOL
            "bigint" -> return ColumnType.COLUMN_TYPE_INT64
            "smallint" -> return ColumnType.COLUMN_TYPE_INT32
            "integer" -> return ColumnType.COLUMN_TYPE_INT32
            "numeric", "real", "double precision" -> return ColumnType.COLUMN_TYPE_DOUBLE
            "bytea", "bit", "bit varying" -> return ColumnType.COLUMN_TYPE_BINARY
            "json", "jsonb" -> return ColumnType.COLUMN_TYPE_JSON
            "daterange", "int4range", "int8range", "numrange", "point", "tsrange",
            "tstzrange", "xml", "inet", "cidr", "macaddr", "oid" ->
                return ColumnType.COLUMN_TYPE_STRING
            else -> return ColumnType.COLUMN_TYPE_STRING
        }
    }

    fun listTablesQuery(schema: String): String {
        val schemaCondition = if (schema != "*") {
            "AND ns.nspname = ?"
        } else {
            "AND ns.nspname NOT IN (${pgSystemTableNames.joinToString { "'$it'" }})"
        }

        return """
SELECT
    ns.nspname,
    c.relname::TEXT,
    c.relkind::TEXT,
    CASE
        WHEN relkind = 'p' THEN (
            SELECT COALESCE(SUM(child.reltuples), 0)
            FROM
                pg_inherits
                JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
                JOIN pg_class child ON pg_inherits.inhrelid = child.oid
            WHERE parent.oid = c.oid
        )
        ELSE c.reltuples
    END
FROM
    pg_class c
    INNER JOIN pg_namespace ns ON c.relnamespace = ns.oid
WHERE
	has_schema_privilege(ns.oid, 'USAGE')
	AND has_table_privilege(c.oid, 'SELECT')
    $schemaCondition
    AND c.relname NOT IN (${pgSystemSchemas.joinToString { "'$it'" }})
    AND c.relkind = 'r'
        """
    }

    fun listTableSchemaQuery(): String {
        return """
SELECT column_name, data_type, column_default, is_nullable
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = ? AND table_name = ?;
        """
    }

    fun queryTableKey(): String {
        return """
WITH
	table_names AS (
		SELECT
			c.oid AS cl,
			ns.nspname AS table_schema,
			c.relname::TEXT AS table_name
		FROM
			pg_class c
			INNER JOIN pg_namespace ns ON c.relnamespace = ns.oid
		WHERE 
            ns.nspname = ? AND c.relname = ?
            AND c.relkind = 'r'
	),
	unique_indexes AS (
		SELECT DISTINCT ON (indrelid)
			indrelid,
			indkey
		FROM pg_index
		WHERE
			indpred IS NULL
			and indisunique
			and indisreplident
		ORDER BY indrelid, indnatts ASC, indexrelid
	),
	pkeys AS (
		SELECT
			conrelid,
			conkey
		FROM pg_constraint
		WHERE contype = 'p'
	),
	replica_identity AS (
		SELECT
			COALESCE(conrelid, indrelid) as relid,
			COALESCE(conkey, indkey) as key
		FROM unique_indexes
		FULL OUTER JOIN pkeys ON indrelid = conrelid
	),
	primary_keys_columns AS (
		SELECT
			UNNEST(replica_identity.key) AS col_num,
			table_names.table_name,
			table_names.table_schema,
			table_names.cl
		FROM replica_identity
		JOIN table_names ON replica_identity.relid = table_names.cl
	),
	ordered_primary_keys_columns AS (
		SELECT
			ROW_NUMBER() OVER () AS row_number,
			col_num,
			table_name,
			table_schema,
			cl
		FROM primary_keys_columns
	)
SELECT
	table_schema,
	table_name,
	attname
FROM
	pg_attribute
	JOIN ordered_primary_keys_columns ON (attrelid = cl AND col_num = attnum)
WHERE
	has_column_privilege(ordered_primary_keys_columns.cl, ordered_primary_keys_columns.col_num, 'SELECT')
ORDER BY row_number 
        """
    }
}

data class Wal2JsonMessage(
    val xid: Long,
    val nextLsn: String,
    val timestamp: String,
    val change: List<Wal2JsonChange>
) {
    fun getDataChangeItems(): List<DataChangeItem> = change.map { it.toDataChangeItem() }
}

data class Wal2JsonChange(
    val kind: String,
    val schema: String,
    val table: String,
    val columnnames: List<String>,
    val columntypes: List<String>,
    val columnvalues: List<String>,
    var oldkeys: Wal2JsonKeyChange
) {
    private fun getColumnValue(columnType: String, columnValue: String): ColumnValue {
        val columnBuilder = ColumnValue.newBuilder()

        val pgPrefix = "pg:"
        val pgTypeTrim = if (columnType.startsWith(pgPrefix)) {
            columnType.substring(pgPrefix.length)
        } else columnType

        if (pgTypeTrim.startsWith("character") or pgTypeTrim.startsWith("character varying")) {
            return columnBuilder.setString(columnValue).build()
        }
        if (pgTypeTrim.startsWith("bit(") or pgTypeTrim.startsWith("bit varying(")) {
            return columnBuilder.setBinary(ByteString.copyFromUtf8(columnValue)).build()
        }
        when (pgTypeTrim) {
            "timestamp without time zone", "timestamp with time zone", "time without time zone", "time with time zone", "date"
            -> return columnBuilder.setIsoTime(columnValue).build()
            "uuid", "name", "text", "interval", "char", "abstime", "money"
            -> return columnBuilder.setString(columnValue).build()
            "boolean" -> return columnBuilder.setBool(Klaxon().parse<Boolean>(columnValue)!!).build()
            "bigint" -> return columnBuilder.setInt64(Klaxon().parse<Long>(columnValue)!!).build()
            "integer", "smallint" -> return columnBuilder.setInt32(Klaxon().parse<Int>(columnValue)!!).build()
            "numeric", "real", "double precision" -> return columnBuilder.setBigDecimal(columnValue).build()
            "bytea", "bit", "bit varying" -> columnBuilder.setBinary(ByteString.copyFromUtf8(columnValue)).build()
            "json", "jsonb" -> return columnBuilder.setJson(columnValue).build()
            "daterange", "int4range", "int8range", "numrange", "point", "tsrange",
            "tstzrange", "xml", "inet", "cidr", "macaddr", "oid" ->
                return columnBuilder.setString(columnValue).build()
            else -> return columnBuilder.setString(columnValue).build()
        }
        return columnBuilder.build()
    }

    fun toPlainRow(): PlainRow {
        val rowBuilder = PlainRow.newBuilder()
        columntypes.zip(columnvalues).map {
            val value = this.getColumnValue(it.first, it.second)
            rowBuilder.addValues(value)
        }
        return rowBuilder.build()
    }

    fun toSchema(): Schema {
        val columns = columnnames.zip(columntypes).map {
            Column.newBuilder()
                .setName(it.first)
                .setType(PsqlQueries.parsePgType(it.second))
                .setKey(oldkeys.keynames.contains(it.first))
                .setOriginalType(
                    Column.OriginalType.newBuilder()
                        .setConnectorId(connectorId)
                        .setTypeName(it.second)
                        .build()
                )
                .build()
        }
        return Schema.newBuilder().addAllColumns(columns).build()
    }

    fun toDataChangeItem(): DataChangeItem {
        val opType = when (kind) {
            "insert" -> OpType.OP_TYPE_INSERT
            "update" -> OpType.OP_TYPE_UPDATE
            "delete" -> OpType.OP_TYPE_DELETE
            else -> throw DtExtensionException("Unknown WAL kind: $kind")
        }
        return DataChangeItem.newBuilder()
            .setOpType(opType)
            .setSchema(this.toSchema())
            .setPlainRow(this.toPlainRow())
            .build()
    }
}

data class Wal2JsonKeyChange(
    val keynames: List<String>,
    val keytypes: List<String>,
    val keyvalues: List<Any>,
)

data class PostgreSQLParameters(
    val jdbc_conn_string: String,
    val user: String,
    val password: String,
)

class PostgreSQL : SourceServiceGrpcKt.SourceServiceCoroutineImplBase() {
    private val specificationPath = "/spec.json";

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

    private fun connectToPostgreSQL(jsonSpec: String): Connection {
        val parameters = Klaxon().parse<PostgreSQLParameters>(jsonSpec)
            ?: throw DtExtensionException("Parameters cannot be empty")

        return DriverManager.getConnection(parameters.jdbc_conn_string, parameters.user, parameters.password)
    }

    private fun schemaQuery(connection: Connection, namespace: String, name: String): Schema {
        val schemaQuery = connection.prepareStatement(PsqlQueries.listTableSchemaQuery())
        schemaQuery.setString(1, namespace)
        schemaQuery.setString(2, name)
        val schemaResult = schemaQuery.executeQuery()

        val columns = mutableMapOf<String, Column.Builder>()
        while (schemaResult.next()) {
            val columnName = schemaResult.getString(1)
            val dataType = schemaResult.getString(2)

            columns[columnName] = Column.newBuilder()
                .setName(columnName)
                .setType(PsqlQueries.parsePgType(dataType))
                .setOriginalType(
                    Column.OriginalType.newBuilder()
                        .setConnectorId(connectorId)
                        .setTypeName(dataType)
                        .build()
                )
        }
        schemaResult.close()


        val pkeyQuery = connection.prepareStatement(PsqlQueries.queryTableKey())
        pkeyQuery.setString(1, namespace)
        pkeyQuery.setString(2, name)
        val pkeyQueryResult = pkeyQuery.executeQuery()
        while (pkeyQueryResult.next()) {
            val columnName = pkeyQueryResult.getString(3)
            columns[columnName]?.key = true
        }
        pkeyQueryResult.close()

        return Schema.newBuilder()
            .addAllColumns(columns.map { it.value.build() })
            .build()
    }

    private fun extractMaxColumnInWindow(
        cursor: ColumnCursor, namespace: String, name: String, window: Int) : String{
        val whereClause = cursor.dataRange.from?.let {
            if (it.dataCase == ColumnValue.DataCase.DATA_NOT_SET) {
                ""
            } else if (cursor.dataRange.excludeFrom) {
                "\"${cursor.column.name}\" > ${columnValueAsSqlString(it)}"
            } else {
                "\"${cursor.column.name}\" >= ${columnValueAsSqlString(it)}"
            }
        }
        return """
            SELECT max("${cursor.column.name}")
            FROM (
              SELECT "${cursor.column.name}"
              FROM "$namespace"."$name"
              WHERE $whereClause
              LIMIT $window
            ) as sq;
            """
    }

    private fun deltaTableQuery(
        cursor: ColumnCursor, namespace: String, name: String,
        schema: Schema
    ): String {
        val columnList = schema.columnsList.joinToString { "\"${it.name}\"" }
        val leftWhere = cursor.dataRange.from?.let {
            if (it.dataCase == ColumnValue.DataCase.DATA_NOT_SET) {
                ""
            } else if (cursor.dataRange.excludeFrom) {
                "AND \"${cursor.column.name}\" > ${columnValueAsSqlString(it)}"
            } else {
                "AND \"${cursor.column.name}\" >= ${columnValueAsSqlString(it)}"
            }
        } ?: ""
        val rightWhere = cursor.dataRange.to?.let {
            if (it.dataCase == ColumnValue.DataCase.DATA_NOT_SET) {
                ""
            }else if (cursor.dataRange.excludeTo) {
                "AND \"${cursor.column.name}\" < ${columnValueAsSqlString(it)}"
            } else {
                "AND \"${cursor.column.name}\" <= ${columnValueAsSqlString(it)}"
            }
        } ?: ""
        val query = """
            SELECT $columnList
            FROM "$namespace"."$name"
            WHERE 1=1
            $leftWhere
            $rightWhere
            ORDER BY ${cursor.column.name} ${
            if (cursor.descending) {
                "desc"
            } else ""
        }
            """
        return query
    }

    private fun getColumnValue(result: ResultSet, id: Int, columnType: ColumnType): ColumnValue {
        val columnBuilder = ColumnValue.newBuilder()
        when (columnType) {
            ColumnType.COLUMN_TYPE_BOOL -> return columnBuilder.setBool(result.getBoolean(id)).build()
            ColumnType.COLUMN_TYPE_INT32 -> return columnBuilder.setInt32(result.getInt(id)).build()
            ColumnType.COLUMN_TYPE_INT64 -> return columnBuilder.setInt64(result.getLong(id)).build()
            ColumnType.COLUMN_TYPE_UINT32 -> return columnBuilder.setUint32(result.getInt(id)).build()
            ColumnType.COLUMN_TYPE_UINT64 -> return columnBuilder.setUint64(result.getLong(id)).build()
            ColumnType.COLUMN_TYPE_FLOAT -> return columnBuilder.setFloat(result.getFloat(id)).build()
            ColumnType.COLUMN_TYPE_DOUBLE -> return columnBuilder.setDouble(result.getDouble(id)).build()
            ColumnType.COLUMN_TYPE_JSON -> return columnBuilder.setJson(result.getString(id) ?: "").build()
            ColumnType.COLUMN_TYPE_DECIMAL -> {
                val bigDecimal = result.getBigDecimal(id)
                return columnBuilder.setDecimal(
                    Decimal.newBuilder()
                        .setAsString(bigDecimal.toString())
                        .setPrecision(bigDecimal.precision())
                        .setScale(bigDecimal.scale())
                        .build()
                ).build()
            }
            ColumnType.COLUMN_TYPE_BIG_DECIMAL,
            ColumnType.COLUMN_TYPE_BIG_INTEGER -> {
                return columnBuilder.setBigDecimal(result.getBigDecimal(id).toString()).build()
            }
            ColumnType.COLUMN_TYPE_UNIX_TIME -> return columnBuilder.setUnixTime(result.getTimestamp(id).time).build()
            ColumnType.COLUMN_TYPE_ISO_TIME -> {
                // https://mkyong.com/java/how-to-get-current-timestamps-in-java/
                val date = result.getTimestamp(id)
                val isoFormat = date.toInstant().toString()
                return columnBuilder.setString(isoFormat).build()
            }
            ColumnType.COLUMN_TYPE_STRING -> return columnBuilder.setString(result.getString(id) ?: "").build()
            ColumnType.COLUMN_TYPE_BINARY,
            ColumnType.COLUMN_TYPE_UNSPECIFIED,
            ColumnType.UNRECOGNIZED -> {
                return columnBuilder.setBinary(ByteString.readFrom(result.getBinaryStream(id))).build()
            }
        }
    }

    private fun columnValueAsSqlString(columnValue: ColumnValue): String {
        return when (columnValue.dataCase) {
            ColumnValue.DataCase.BOOL -> columnValue.bool.toString()
            ColumnValue.DataCase.INT32 -> columnValue.int32.toString()
            ColumnValue.DataCase.INT64 -> columnValue.int64.toString()
            ColumnValue.DataCase.UINT32 -> columnValue.uint32.toString()
            ColumnValue.DataCase.UINT64 -> columnValue.uint64.toString()
            ColumnValue.DataCase.FLOAT -> columnValue.float.toString()
            ColumnValue.DataCase.DOUBLE -> columnValue.double.toString()
            ColumnValue.DataCase.JSON -> columnValue.json
            ColumnValue.DataCase.DECIMAL -> columnValue.decimal.asString
            ColumnValue.DataCase.BIG_DECIMAL -> columnValue.bigDecimal
            ColumnValue.DataCase.BIG_INTEGER -> columnValue.bigInteger
            ColumnValue.DataCase.UNIX_TIME -> {
                val iso = java.time.Instant.ofEpochSecond(columnValue.unixTime).toString()
                "'${iso}' ::TIMESTAMP WITH TIME ZONE"
            }
            ColumnValue.DataCase.ISO_TIME -> {
                "'${columnValue.isoTime}' ::TIMESTAMP WITH TIME ZONE"
            }
            ColumnValue.DataCase.STRING -> columnValue.string
            ColumnValue.DataCase.BINARY -> throw DtExtensionException("Binary data is supposed to be non-printable")
            null, ColumnValue.DataCase.DATA_NOT_SET ->
                throw DtExtensionException("No data to convert to SQL representation")
        }
    }

    private fun getAscendingCursor(
        connection: Connection,
        namespace: String,
        name: String,
        column: Column,
        wholePrimaryKey: Boolean = false
    ): Cursor {
        val query = "SELECT max(\"${column.name}\") FROM \"${namespace}\".\"${name}\""
        val stmt = connection.prepareStatement(query)
        val result = stmt.executeQuery()


        if (!result.next()) {
            throw DtExtensionException("Cannot get min/max for column: empty result set")
        }
        if (result.getObject(1) == null) {
            // no actual data range: make column cursor with no data range
            return Cursor.newBuilder().setColumnCursor(
                ColumnCursor.newBuilder().setColumn(column).build()
            ).build()
        }
        val rangeMax = this.getColumnValue(result, 1, column.type)
        return Cursor.newBuilder().setColumnCursor(
            ColumnCursor.newBuilder()
                .setColumn(column)
                .setDataRange(
                    Common.DataRange.newBuilder()
                        // NOTE: exclude left interval if column is really key by itself, not a part of some key
                        .setExcludeFrom(wholePrimaryKey)
                        .setTo(rangeMax)
                        .build()
                )
                // NOTE: we are setting ascending cursor, so client would like to see lower values first
                .setDescending(false)
                .build()
        ).build()
    }

    // TODO make analogue with parquet
    // returns delta with desired window size and
    // maximum cursor
    private fun getTableDeltaAsPlainRow(
        connection: Connection, cursor: Cursor, namespace: String, name: String,
        schema: Schema, window: Int
    ): Pair<List<PlainRow>, ColumnValue> {
        val columnCursor = cursor.columnCursor
            ?: throw DtExtensionException("Only column cursor is supported in connector")

        // cursor is defined as [a, b]. We need to find c as maximum of column value of
        // desired window size
        val maxWindowQuery = extractMaxColumnInWindow(columnCursor, namespace, name, window)
        val maxWindowStmt = connection.prepareStatement(maxWindowQuery)
        val maxWindowResult = maxWindowStmt.executeQuery()
        val maxColumnValue = getColumnValue(maxWindowResult, 1, columnCursor.column.type)

        // we calculated c, the next step is to guarantee, that whole interval [a, c] will be transfered
        val deltaCursor = columnCursor.toBuilder().setDataRange(
                columnCursor.dataRange.toBuilder().setTo(maxColumnValue).setExcludeTo(false)
            ).build()

        val query = deltaTableQuery(deltaCursor, namespace, name, schema)
        val stmt = connection.prepareStatement(query)
        val result = stmt.executeQuery()

        val changeItemList = mutableListOf<PlainRow>()
        while (result.next()) {
            val rowBuilder = PlainRow.newBuilder()
            for (id in 0 until result.metaData.columnCount) {
                val value = getColumnValue(result, id + 1, schema.columnsList.get(id).type)
                rowBuilder.addValues(value)
            }
            changeItemList.add(rowBuilder.build())
        }
        result.close()
        return Pair(changeItemList, maxColumnValue)
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

    override suspend fun discover(request: SourceServiceOuterClass.DiscoverReq): SourceServiceOuterClass.DiscoverRsp {
        try {
            this.ValidateSpec(request.jsonSettings)
            val connection = this.connectToPostgreSQL(request.jsonSettings)

            val query = connection
                .prepareStatement(PsqlQueries.listTablesQuery("*"))

            val result = query.executeQuery()

            val tables = mutableListOf<Table>()

            while (result.next()) {
                val namespace = result.getString(1)
                val name = result.getString(2)

                // retrieve schema for each table
                val schema = this.schemaQuery(connection, namespace, name)
                tables.add(
                    Table.newBuilder()
                        .setNamespace(Namespace.newBuilder().setNamespace(namespace))
                        .setName(name)
                        .setSchema(schema)
                        .build()
                )
            }
            result.close()

            return SourceServiceOuterClass.DiscoverRsp.newBuilder()
                .setResult(RspUtil.resultOk)
                .addAllTables(tables)
                .build()
        } catch (e: java.lang.Exception) {
            return SourceServiceOuterClass.DiscoverRsp.newBuilder()
                .setResult(RspUtil.resultError("exception occured: ${e.message}"))
                .build()
        }
    }

    override fun read(requests: Flow<SourceServiceOuterClass.ReadReq>): Flow<ReadRsp> {
        fun mkRsp(controlItem: Any): ReadRsp {
            val readCtlRsp = ReadCtlRsp.newBuilder()
            when (controlItem) {
                is InitRsp -> readCtlRsp.initRsp = controlItem
                is CursorRsp -> readCtlRsp.cursorRsp = controlItem
                is BeginSnapshotRsp -> readCtlRsp.beginSnapshotRsp = controlItem
                is ReadChangeRsp -> readCtlRsp.readChangeRsp = controlItem
                is DoneSnapshotRsp -> readCtlRsp.doneSnapshotRsp = controlItem
                else -> throw IllegalArgumentException("Unknown control item type: ${controlItem.javaClass}")
            }
            return ReadRsp.newBuilder()
                .setResult(RspUtil.resultOk)
                .setReadCtlRsp(readCtlRsp)
                .build()
        }

        fun mkBadRsp(error: String): ReadRsp =
            ReadRsp.newBuilder().setResult(RspUtil.resultError(error)).build()

        return flow {
            // INFO: this is the only stateful resources per-gRPC call: connection to database and client ID
            // It is initialized when INIT_CONNECTION_REQ message with full endpoint specification comes
            lateinit var connection: Connection
            // note, that you can use clientId to persist resources in database for particular client,
            // or if you write completely stateless connector, you may ignore clientId at all
            lateinit var clientId: String

            requests.collect { req ->
                val table = req.table
                val namespace = table.namespace.namespace
                val name = table.name
                try {
                    when (req.readCtlReq?.ctlReqCase) {
                        ReadCtlReq.CtlReqCase.INIT_REQ -> {
                            // STEP 1: this branch should initialize connection with database
                            // THIS STAGE IS NEVER SKIPPED BY CLIENT AFTER EACH gRPC REQUEST!
                            //
                            // This is the first control message that should be sent by client
                            // that established connection with service in order to
                            // service be able to establish connection with database
                            val initConnReq = req.readCtlReq.initReq
                            clientId = initConnReq.clientId ?: UUID.randomUUID().toString()
                            // check spec and initialize connection (as in previous handles)
                            this@PostgreSQL.ValidateSpec(initConnReq.jsonSettings)
                            connection = this@PostgreSQL.connectToPostgreSQL(initConnReq.jsonSettings)
                            emit(mkRsp(InitRsp.newBuilder().setClientId(clientId).build()))
                        }
                        ReadCtlReq.CtlReqCase.CURSOR_REQ -> {
                            // STEP 2: then client should know, how to request ranges of data,
                            // that's why you should provide him cursor
                            // NOTE: client can skip this message after reconnect if it already has some cursor
                            //
                            // note, that expected cursor interval has no minimum value, because
                            // minimum value of interval is excluded and interpreted as last
                            // commited
                            // and selection of pieces is happened as select statement
                            val cursorReq = req.readCtlReq.cursorReq
                            val schema = this@PostgreSQL.schemaQuery(connection, namespace, name)

                            val colName = cursorReq.preferredColumn
                            val keyCount = schema.columnsList.count { it.key }
                            val col =
                                schema.columnsList.find { it.name == colName && colName != "" }
                                    ?: schema.columnsList.find { it.key }
                                    ?: schema.columnsList.find { it.type == ColumnType.COLUMN_TYPE_INT64 }
                                    ?: schema.columnsList.find { it.type == ColumnType.COLUMN_TYPE_INT32 }
                                    ?: schema.columnsList.first()
                                    ?: throw DtExtensionException("Couldn't find any matching column... suggest yours")

                            val newCursor = getAscendingCursor(
                                connection,
                                namespace,
                                name,
                                col,
                                // if this column is a single key: safely exclude lower bound of cursor
                                keyCount == 1
                            )
                            emit(mkRsp(CursorRsp.newBuilder().setCursor(newCursor).build()))
                        }
                        ReadCtlReq.CtlReqCase.BEGIN_SNAPSHOT_REQ -> {
                            // STEP 3: this is just a notification when uploading of the table is starting
                            // NOTE: client can skip this step if it already began to upload table, but reconnection happened
                            val state = "Hello, world!"
                            print("Setting begin snapshot state: $state")
                            val controlItem =
                                BeginSnapshotRsp.newBuilder()
                                    // you may set here additional state that will be restored on DONE_SNAPSHOT_REQ
                                    .setSnapshotState(ByteString.copyFromUtf8(state))
                                    .build()
                            emit(mkRsp(controlItem))
                        }
                        ReadCtlReq.CtlReqCase.READ_CHANGE_REQ -> {
                            // STEP 4: the main routine
                            // Client would like to request corresponding lines for table
                            //
                            // Note that you should emit multiple messages until cursor change,
                            // otherwise you will be requested the same data range again.
                            //
                            // Also note, that you may emit here more than one data change item:
                            // that is increase efficiency of using network. For example, you may
                            // transmit 1000 items back to client. You can also pack your change
                            // items in parquet format to be even more efficient
                            val readChangeReq = req.readCtlReq.readChangeReq
                            val cursor = readChangeReq.cursor
                            val schema = this@PostgreSQL.schemaQuery(connection, namespace, name)
                            // TODO rework delta extraction for cursor commitment
                            val (changeItems, maxColumn) = this@PostgreSQL.getTableDeltaAsPlainRow(
                                connection, cursor, namespace, name, schema, 1000
                            )
                            val columnCursor = cursor.columnCursor
                                ?: throw DtExtensionException("Only column cursors are supported")

                            val colCursorId = schema.columnsList.indexOfFirst { it.name == columnCursor.column.name }
                            if (colCursorId == -1) throw DtExtensionException("Column cursor not found in schema")

                            changeItems.forEach {
                                // emit change items
                                val controlItem =
                                    ReadChangeRsp.newBuilder().setChangeItem(
                                        ChangeItem.newBuilder().setDataChangeItem(
                                            DataChangeItem.newBuilder()
                                                .setSchema(schema)
                                                .setOpType(OpType.OP_TYPE_INSERT)
                                                .setPlainRow(it)
                                        )
                                    )
                                    .build()
                                emit(mkRsp(controlItem))
                            }
                            // demarcate your end: respond with new data range still not transferred
                            emit(
                                mkRsp(
                                    ReadChangeRsp.newBuilder().setEndOfRead(
                                        ReadChangeRsp.EndOfRead.newBuilder().setCursor(
                                            Cursor.newBuilder().setColumnCursor(
                                                columnCursor.toBuilder().setDataRange(
                                                    columnCursor.dataRange.toBuilder()
                                                        .setFrom(maxColumn)
                                                        .setExcludeFrom(true)
                                                )
                                            )
                                        )
                                    ).build()
                                )
                            )

                        }
                        ReadCtlReq.CtlReqCase.DONE_SNAPSHOT_REQ -> {
                            // STEP 5: when upload of the table is over, you will be notified by client
                            // You can clean resources if you created some in BEGIN_SNAPSHOT_REQ
                            val doneSnapshotReq = req.readCtlReq.doneSnapshotReq
                            // If you saved some state during INIT_SNAPSHOT_REQ, you can use it here.
                            // Motivation why you should do it like this, but not with local variable
                            // is reconnections. State is represented by request, not by the state of the program
                            print("Restored snapshot state by client: ${doneSnapshotReq.snapshotState.toStringUtf8()}")
                            emit(
                                mkRsp(DoneSnapshotRsp.getDefaultInstance())
                            )
                        }
                        null, ReadCtlReq.CtlReqCase.CTLREQ_NOT_SET ->
                            emit(mkBadRsp("no control item response"))
                    }
                } catch (e: java.lang.Exception) {
                    emit(mkBadRsp("exception occured: ${e.message}; full: ${e.toString()}"))
                }
            }
        }
    }

    override fun stream(requests: Flow<SourceServiceOuterClass.StreamReq>): Flow<StreamRsp> {
        // we'll use this plugin for replication:
        // https://github.com/eulerto/wal2json
        val pgReplicationPlugin = "wal2json"

        fun mkRsp(lsn: ColumnValue, controlItem: Any): StreamRsp {
            val streamCtlRsp = StreamCtlRsp.newBuilder()
            when (controlItem) {
                is InitRsp -> streamCtlRsp.initRsp = controlItem
                is FixLsnRsp -> streamCtlRsp.fixLsnRsp = controlItem
                is CheckLsnRsp -> streamCtlRsp.checkLsnRsp = controlItem
                is StreamChangeRsp -> streamCtlRsp.streamChangeRsp = controlItem
                is RewindLsnReq -> streamCtlRsp.rewindLsnReq = controlItem
                is LostRequestedLsnRsp -> streamCtlRsp.lostRequestedLsnRsp = controlItem
                else -> throw IllegalArgumentException("Unknown control item type: ${controlItem.javaClass}")
            }
            return StreamRsp.newBuilder()
                .setResult(RspUtil.resultOk)
                .setLsn(lsn)
                .setStreamCtlRsp(streamCtlRsp)
                .build()
        }

        fun mkBadRsp(cursor: ColumnValue, error: String): StreamRsp =
            StreamRsp.newBuilder().setLsn(cursor).setResult(RspUtil.resultError(error)).build()

        return flow {
            lateinit var clientId: String
            lateinit var replConnection: PGConnection
            lateinit var stream: PGReplicationStream

            fun genSlotName(clientId: String) = "slot_for_$clientId"

            // use this function monodically on stream, e.g.
            // ```stream = stream.restoreFromLsn(lsn)```
            fun PGReplicationStream?.streamFromLsn(lsn: LogSequenceNumber): PGReplicationStream {
                if (this == null) {
                    val slotName = genSlotName(clientId)
                    // this are parameters specification for plugin
                    val systemSchemasList = PsqlQueries.pgSystemSchemas.joinToString { "\"$it\".*" }
                    val systemTablesList = PsqlQueries.pgSystemSchemas.joinToString { "*.\"$it\"" }
                    return replConnection.replicationAPI
                        .replicationStream()
                        .logical()
                        .withSlotName(slotName)
                        .withSlotOption("include-xids", true)
                        .withSlotOption("include-lsn", true)
                        .withSlotOption("include-timestamp", true)
                        .withSlotOption("include-transaction", true)
                        .withSlotOption("filter-tables", "$systemSchemasList,$systemTablesList")
                        .withStartPosition(lsn)
                        .start()
                }
                this.setAppliedLSN(lsn)
                this.setFlushedLSN(lsn)
                // ping that we're still here
                this.forceUpdateStatus()
                return this
            }

            requests.collect { req ->
                val lsn = req.lsn
                try {
                    when (req.streamCtlReq?.controlItemReqCase) {
                        StreamCtlReq.ControlItemReqCase.INIT_REQ -> {
                            // STEP 1: this branch should initialize connection with database
                            // THIS STAGE IS NEVER SKIPPED BY CLIENT AFTER EACH gRPC REQUEST!
                            //
                            // This is the first control message that should be sent by client
                            // that established connection with service in order to
                            // service be able to establish connection with database
                            val initConnReq = req.streamCtlReq.initReq
                            clientId = initConnReq.clientId ?: UUID.randomUUID().toString()
                            // check spec and initialize connection (as in previous handles)
                            this@PostgreSQL.ValidateSpec(initConnReq.jsonSettings)
                            val vanillaConnection = this@PostgreSQL.connectToPostgreSQL(initConnReq.jsonSettings)
                            replConnection = vanillaConnection.unwrap(PGConnection::class.java)
                            emit(mkRsp(lsn, InitRsp.newBuilder().setClientId(clientId).build()))
                        }
                        StreamCtlReq.ControlItemReqCase.FIX_LSN_REQ -> {
                            // STEP 2: the next thing client will want is to fix LSN position
                            // This can be skipped if Client have already done this step and
                            // didn't send request for REWIND_LSN_REQ, or client received
                            // LostRequestedLsnRsp: un such cases client may request to fix LSN again
                            val fixLsnReq = req.streamCtlReq.fixLsnReq
                            when (fixLsnReq.streamSource.sourceCase) {
                                FixLsnReq.StreamSource.SourceCase.CLUSTER -> {
                                    // OK
                                }
                                FixLsnReq.StreamSource.SourceCase.TABLE,
                                FixLsnReq.StreamSource.SourceCase.NAMESPACE,
                                null, FixLsnReq.StreamSource.SourceCase.SOURCE_NOT_SET -> {
                                    throw DtExtensionException("Only whole cluster cursor is acceptable")
                                }
                            }

                            val slotName = genSlotName(clientId)
                            val slot = replConnection.getReplicationAPI()
                                .createReplicationSlot()
                                .logical()
                                .withSlotName(slotName)
                                .withOutputPlugin(pgReplicationPlugin)
                                .make()

                            // after creating slot, save it's LSN in the format of column
                            val slotLsn = ColumnValue.newBuilder().setString(slot.consistentPoint.asString()).build()

                            // you can identify allocated resources here, for instance,
                            // you may save slot name that was just created
                            // but this is not needed in current implementation
                            val replicationState = ByteString.copyFromUtf8("Hello, world!")
                            emit(
                                mkRsp(
                                    slotLsn, FixLsnRsp.newBuilder()
                                        .setReplicationState(replicationState)
                                        .build()
                                )
                            )
                        }
                        StreamCtlReq.ControlItemReqCase.CHECK_LSN_REQ -> {
                            // normally, we should check if LSN still here, but
                            // I don't know how to do it yet.
                            // But client should periodically (e.g. once in 5 seconds)
                            // ping that handle in order to make connection alive
                            val pingLsn = LogSequenceNumber.valueOf(lsn.string)
                            stream = stream.streamFromLsn(pingLsn)
                        }
                        StreamCtlReq.ControlItemReqCase.STREAM_CHANGE_REQ -> {
                            val waitLSN = LogSequenceNumber.valueOf(lsn.string)
                            if (waitLSN == LogSequenceNumber.INVALID_LSN) {
                                throw DtExtensionException("Invalid LSN format.")
                            }
                            stream = stream.streamFromLsn(waitLSN)
                            val wal2jsonBuf = stream.read()
                                ?: throw DtExtensionException("wal2json returned null byte buffer")

                            val wal2jsonStr = StandardCharsets.UTF_8.decode(wal2jsonBuf).toString()
                            val wal2json = Klaxon().parse<Wal2JsonMessage>(wal2jsonStr)
                                ?: throw DtExtensionException("Wrong format of wal2json message")

                            wal2json.getDataChangeItems().forEach {
                                emit(
                                    mkRsp(
                                        lsn, StreamChangeRsp.newBuilder()
                                            .setChangeItem(
                                                ChangeItem.newBuilder().setDataChangeItem(it)
                                            )
                                            .build()
                                    )
                                )
                            }

                            // don't forget to send final message
                            val nextLsnCol = ColumnValue.newBuilder().setString(wal2json.nextLsn).build()
                            emit(mkRsp(nextLsnCol,
                                StreamChangeRsp.newBuilder()
                                    .setEndOfStream(EndOfStream.getDefaultInstance())
                                    .build()
                            )
                            )

                        }
                        StreamCtlReq.ControlItemReqCase.REWIND_LSN_REQ -> {
                            val rewindLsnReq = req.streamCtlReq.rewindLsnReq
                            print("Restored snapshot state by client: ${rewindLsnReq.replicationState.toStringUtf8()}")
                            // drop allocated resource: replication slot
                            val slotName = genSlotName(clientId)
                            replConnection.getReplicationAPI()
                                .dropReplicationSlot(slotName)
                            emit(mkRsp(lsn, RewindLsnRsp.getDefaultInstance()))
                        }
                        null, StreamCtlReq.ControlItemReqCase.CONTROLITEMREQ_NOT_SET ->
                            emit(mkBadRsp(lsn, "no control item response"))
                    }
                } catch (e: java.lang.Exception) {
                    emit(mkBadRsp(lsn, "exception occured: ${e.message}"))
                }
            }
        }
    }
}