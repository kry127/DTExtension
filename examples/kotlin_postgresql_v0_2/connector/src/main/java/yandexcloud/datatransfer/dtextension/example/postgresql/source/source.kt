package yandexcloud.datatransfer.dtextension.example.postgresql.source

import com.beust.klaxon.Klaxon
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import net.pwall.json.schema.JSONSchema
import yandexcloud.datatransfer.dtextension.example.postgresql.*
import yandexcloud.datatransfer.dtextension.v0_2.Common
import yandexcloud.datatransfer.dtextension.v0_2.Common.ColumnCursor
import yandexcloud.datatransfer.dtextension.v0_2.Common.Cursor
import yandexcloud.datatransfer.dtextension.v0_2.Common.EndCursor
import yandexcloud.datatransfer.dtextension.v0_2.Common.InitRsp
import yandexcloud.datatransfer.dtextension.v0_2.Data.*
import yandexcloud.datatransfer.dtextension.v0_2.source.Read.*
import yandexcloud.datatransfer.dtextension.v0_2.source.Stream.*
import yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceGrpcKt
import yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceOuterClass
import yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceOuterClass.ReadRsp
import yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceOuterClass.StreamRsp
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.util.*



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
                return ColumnType.COLUMN_TYPE_UNIX_TIME
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
            "AND ns.nspname NOT IN (${pgSystemSchemas.joinToString { "'$it'" }})"
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
    val nextlsn: String,
    val timestamp: String,
    val change: List<Wal2JsonChange>
) {
    fun getDataChangeItems(): List<DataChangeItem> = change.map { it.toDataChangeItem() }
    fun unixTimestamp() : Long {
        // val template = "2006-01-02 15:04:05.999999999-07"
        val sdf = java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSSSSX")
        return sdf.parse(this.timestamp).time / 1000
    }
}

data class Wal2JsonChange(
    val kind: String,
    val schema: String,
    val table: String,
    val columnnames: List<String> = listOf(),
    val columntypes: List<String> = listOf(),
    val columnvalues: List<Any> = listOf(),
    var oldkeys: Wal2JsonKeyChange = Wal2JsonKeyChange(listOf(), listOf(), listOf()),
) {
    private fun getColumnValue(columnType: String, columnValue: Any): ColumnValue {
        val columnBuilder = ColumnValue.newBuilder()

        val pgPrefix = "pg:"
        val pgTypeTrim = if (columnType.startsWith(pgPrefix)) {
            columnType.substring(pgPrefix.length)
        } else columnType

        if (pgTypeTrim.startsWith("character") or pgTypeTrim.startsWith("character varying")) {
            return columnBuilder.setString(columnValue.toString()).build()
        }
        if (pgTypeTrim.startsWith("bit(") or pgTypeTrim.startsWith("bit varying(")) {
            return columnBuilder.setBinary(ByteString.copyFromUtf8(columnValue.toString())).build()
        }
        when (pgTypeTrim) {
            "timestamp without time zone", "timestamp with time zone", "time without time zone", "time with time zone", "date"
            -> return columnBuilder.setUnixTime(columnValue as Long).build()
            "uuid", "name", "text", "interval", "char", "abstime", "money"
            -> return columnBuilder.setString(columnValue.toString()).build()
            "boolean" -> return columnBuilder.setBool(columnValue as Boolean).build()
            "bigint" -> return columnBuilder.setInt64(columnValue as Long).build()
            "integer", "smallint" -> return columnBuilder.setInt32(columnValue as Int).build()
            "numeric", "real", "double precision" -> return columnBuilder.setBigDecimal(columnValue as String).build()
            "bytea", "bit", "bit varying" -> columnBuilder.setBinary(ByteString.copyFromUtf8(columnValue as String)).build()
            "json", "jsonb" -> return columnBuilder.setJson(columnValue as String).build()
            "daterange", "int4range", "int8range", "numrange", "point", "tsrange",
            "tstzrange", "xml", "inet", "cidr", "macaddr", "oid" ->
                return columnBuilder.setString(columnValue as String).build()
            else -> return columnBuilder.setString(columnValue.toString()).build()
        }
        return columnBuilder.build()
    }

    fun toPlainRow(): PlainRow {
        val rowBuilder = PlainRow.newBuilder()
        columntypes.zip(columnvalues).map { (colType, colValue) ->
            val value = this.getColumnValue(colType, colValue)
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

    fun toTable(): Table {
        val schema = this.toSchema()
        return Table.newBuilder().setSchema(schema).setName(this.table).setNamespace(
            Namespace.newBuilder().setNamespace(this.schema)
        ).build()
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
            .setTable(this.toTable())
            .setPlainRow(this.toPlainRow())
            .build()
    }
}

data class Wal2JsonKeyChange(
    val keynames: List<String>,
    val keytypes: List<String>,
    val keyvalues: List<Any>,
)

data class PostgresSourceParameters(
    val jdbc_conn_string: String,
    val user: String,
    val password: String,
)

class PostgresSource : SourceServiceGrpcKt.SourceServiceCoroutineImplBase() {
    private val specificationPath = "/source_spec.json";

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
        val parameters = Klaxon().parse<PostgresSourceParameters>(jsonSpec)
            ?: throw DtExtensionException("Parameters cannot be empty")

        return DriverManager.getConnection(parameters.jdbc_conn_string, parameters.user, parameters.password)
    }

    private fun schemaQuery(connection: Connection, namespace: String, name: String): Schema {
        val schemaQuery = connection.prepareStatement(PsqlQueries.listTableSchemaQuery())
        schemaQuery.setString(1, namespace)
        schemaQuery.setString(2, name)
        val columns = mutableMapOf<String, Column.Builder>()
        schemaQuery.executeQuery().use {schemaResult ->
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
        }


        val pkeyQuery = connection.prepareStatement(PsqlQueries.queryTableKey())
        pkeyQuery.setString(1, namespace)
        pkeyQuery.setString(2, name)
        pkeyQuery.executeQuery().use { pkeyQueryResult ->
            while (pkeyQueryResult.next()) {
                val columnName = pkeyQueryResult.getString(3)
                columns[columnName]?.key = true
            }
        }

        return Schema.newBuilder()
            .addAllColumns(columns.map { it.value.build() })
            .build()
    }

    private fun extractMaxColumnInWindow(
        cursor: ColumnCursor, namespace: String, name: String, window: Int) : String{
        val whereClause = cursor.dataRange.from?.let {
            if (it.dataCase == ColumnValue.DataCase.DATA_NOT_SET) {
                null
            } else if (cursor.dataRange.excludeFrom) {
                "\"${cursor.column.name}\" > ${columnValueAsSqlString(it)}"
            } else {
                "\"${cursor.column.name}\" >= ${columnValueAsSqlString(it)}"
            }
        } ?.let { "WHERE $it"} ?: ""
        return """
            SELECT max("${cursor.column.name}")
            FROM (
              SELECT "${cursor.column.name}"
              FROM "$namespace"."$name"
              $whereClause
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
                null
            } else if (cursor.dataRange.excludeFrom) {
                "AND \"${cursor.column.name}\" > ${columnValueAsSqlString(it)}"
            } else {
                "AND \"${cursor.column.name}\" >= ${columnValueAsSqlString(it)}"
            }
        } ?: ""
        val rightWhere = cursor.dataRange.to?.let {
            if (it.dataCase == ColumnValue.DataCase.DATA_NOT_SET) {
                null
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
            ColumnType.COLUMN_TYPE_UNIX_TIME -> return columnBuilder.setUnixTime(result.getTimestamp(id).time / 1000).build()
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
            ColumnValue.DataCase.JSON -> "'${columnValue.json}'"
            ColumnValue.DataCase.DECIMAL -> columnValue.decimal.asString
            ColumnValue.DataCase.BIG_DECIMAL -> columnValue.bigDecimal
            ColumnValue.DataCase.BIG_INTEGER -> columnValue.bigInteger
            ColumnValue.DataCase.UNIX_TIME -> {
                val iso = java.time.Instant.ofEpochSecond(columnValue.unixTime).toString()
                "'${iso}' ::TIMESTAMP WITH TIME ZONE"
            }
            ColumnValue.DataCase.STRING -> "'${columnValue.string}'"
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
    ): Cursor {
        val query = "SELECT min(\"${column.name}\"), max(\"${column.name}\") FROM \"${namespace}\".\"${name}\""
        val stmt = connection.prepareStatement(query)

        stmt.executeQuery().use{result ->
            if (!result.next()) {
                throw DtExtensionException("Cannot get min/max for column: empty result set")
            }
            if (result.getObject(1) == null) {
                // no actual data range: make end cursor
                return Cursor.newBuilder().setEndCursor(
                    EndCursor.getDefaultInstance()
                ).build()
            }
            val rangeMin = this.getColumnValue(result, 1, column.type)
            val rangeMax = this.getColumnValue(result, 2, column.type)

            return Cursor.newBuilder().setColumnCursor(
                ColumnCursor.newBuilder()
                    .setColumn(column)
                    .setDataRange(
                        Common.DataRange.newBuilder()
                            .setFrom(rangeMin)
                            .setTo(rangeMax)
                            .build()
                    )
                    // NOTE: we are setting ascending cursor, so client would like to see lower values first
                    .setDescending(false)
                    .build()
            ).build()
        }
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
        val maxColumnValue = maxWindowStmt.executeQuery().use { maxWindowResult ->
            if (!maxWindowResult.next()) {
                throw DtExtensionException("max aggregaion should contain at least one value")
            }
            getColumnValue(maxWindowResult, 1, columnCursor.column.type)
        }

        // we calculated c, the next step is to guarantee, that whole interval [a, c] will be transfered
        val deltaCursor = columnCursor.toBuilder().setDataRange(
                columnCursor.dataRange.toBuilder().setTo(maxColumnValue).setExcludeTo(false)
            ).build()

        val query = deltaTableQuery(deltaCursor, namespace, name, schema)
        val stmt = connection.prepareStatement(query)
        stmt.executeQuery().use { result ->
            val changeItemList = mutableListOf<PlainRow>()
            while (result.next()) {
                val rowBuilder = PlainRow.newBuilder()
                for (id in 0 until result.metaData.columnCount) {
                    val value = getColumnValue(result, id + 1, schema.columnsList.get(id).type)
                    rowBuilder.addValues(value)
                }
                changeItemList.add(rowBuilder.build())
            }
            return Pair(changeItemList, maxColumnValue)
        }
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

            val tables = mutableListOf<Table>()

            query.executeQuery().use { result ->
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
            }

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
                            this@PostgresSource.ValidateSpec(initConnReq.jsonSettings)
                            connection = this@PostgresSource.connectToPostgreSQL(initConnReq.jsonSettings)
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
                            val schema = this@PostgresSource.schemaQuery(connection, namespace, name)

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
                                col
                            )
                            emit(mkRsp(CursorRsp.newBuilder().setCursor(newCursor).build()))
                        }
                        ReadCtlReq.CtlReqCase.BEGIN_SNAPSHOT_REQ -> {
                            // STEP 3: this is just a notification when uploading of the table is starting
                            // NOTE: client can skip this step if it already began to upload table, but reconnection happened
                            val state = "Hello, world!"
                            println("Setting begin snapshot state: $state, for: ${req.table}")
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
                            val schema = this@PostgresSource.schemaQuery(connection, namespace, name)
                            // TODO rework delta extraction for cursor commitment
                            val (changeItems, maxColumn) = this@PostgresSource.getTableDeltaAsPlainRow(
                                connection, cursor, namespace, name, schema, 1000
                            )
                            val columnCursor = cursor.columnCursor
                                ?: throw DtExtensionException("Only column cursors are supported")

                            val colCursorId = schema.columnsList.indexOfFirst { it.name == columnCursor.column.name }
                            if (colCursorId == -1) throw DtExtensionException("Column cursor not found in schema")

                            // emit series of change items in row
                            changeItems.forEach {
                                // emit change items
                                val controlItem =
                                    ReadChangeRsp.newBuilder().setChangeItem(
                                        ChangeItem.newBuilder().setDataChangeItem(
                                            DataChangeItem.newBuilder()
                                                .setOpType(OpType.OP_TYPE_INSERT)
                                                .setTable(table)
                                                .setPlainRow(it)
                                        )
                                    )
                                    .build()
                                emit(mkRsp(controlItem))
                            }

                            // define checkpoint cursor
                            val checkpointCursor = if (changeItems.isEmpty()) {
                                // if result is empty, declare as EOF
                                Cursor.newBuilder().setEndCursor(EndCursor.getDefaultInstance())
                            } else {
                                // else, move cursor further and cooperate it to client
                                Cursor.newBuilder().setColumnCursor(
                                    columnCursor.toBuilder().setDataRange(
                                        columnCursor.dataRange.toBuilder()
                                            .setFrom(maxColumn)
                                            .setExcludeFrom(true)
                                    )
                                )
                            }
                            // demarcate end of change item stream: pass the ball to client again
                            emit(
                                mkRsp(
                                    ReadChangeRsp.newBuilder().setCheckpoint(
                                        ReadChangeRsp.CheckPoint.newBuilder().setCursor(checkpointCursor)
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
                            emit(mkBadRsp("no control item request sent"))
                    }
                } catch (e: java.lang.Exception) {
                    emit(mkBadRsp("exception occured: ${e.message}; full: $e"))
                }
            }
        }
    }


    // Working with PostgreSQL WAL LSN, as PgJDBC one is not working
    // See queries here: https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-REPLICATION
    // 1. Creation of slot:
    // SELECT pg_create_logical_replication_slot('test_slot', 'wal2json')
    // 2. Picking changes:
    // SELECT pg_logical_slot_peek_changes('test_slot', NULL, NULL, 'include-xids', 'true', 'actions', 'insert,update,delete', 'include-lsn', 'true', 'include-transaction', 'true');
    // 3. Moving slot:
    // SELECT pg_replication_slot_advance('test_slot', '9/C80001A0')
    // SELECT pg_logical_slot_get_changes('test_slot', '9/C80001A0', NULL, 'include-xids', 'true', 'actions', 'insert,update,delete', 'include-lsn', 'true', 'include-transaction', 'true');
    // 4. Dropping slot
    // SELECT pg_drop_replication_slot('test_slot')
    private fun walLockLsnQuery(slotName: String) : String {
        // we'll use this plugin for replication:
        // https://github.com/eulerto/wal2json
        val pgReplicationPlugin = "wal2json"

        return "SELECT (pg_create_logical_replication_slot('$slotName', '$pgReplicationPlugin')).lsn"
    }

    private fun walGetCommittedLsn(slotName: String) : String {
        return  "SELECT confirmed_flush_lsn FROM pg_replication_slots\n" +
                "WHERE slot_name='$slotName';"
    }

    private fun walPeakChangesQuery(slotName: String, limit: Int) : String {
        return "SELECT (T.pg_logical_slot_peek_changes).lsn, (T.pg_logical_slot_peek_changes).data" +
                " FROM (" +
                "    SELECT pg_logical_slot_peek_changes('$slotName', NULL, $limit," +
                "    'include-xids', 'true'," +
                "    'include-timestamp', 'true'," +
                "    'actions', 'insert,update,delete'," +
                "    'include-lsn', 'true'," +
                "    'include-transaction', 'true')" +
                ") AS T"
    }

    private fun walMoveSlotQuery(slotName: String, lsn: String) : String {
        // no rights on this command in MDB:
        // return "SELECT pg_replication_slot_advance('$slotName', '${lsn}')"
        return "SELECT pg_logical_slot_get_changes('$slotName', '$lsn', NULL," +
                "'include-xids', 'true'," +
                "'include-timestamp', 'true'," +
                "'actions', 'insert,update,delete'," +
                "'include-lsn', 'true'," +
                "'include-transaction', 'true')"
    }

    private fun walDeleteSlotQuery(slotName: String): String {
        return "SELECT pg_drop_replication_slot('$slotName')"
    }

    private fun getCurrentLsn(connection: Connection, slotName: String): String? {
        val peakOneChange = walGetCommittedLsn(slotName)
        val stmt = connection.prepareStatement(peakOneChange)
        stmt.executeQuery().use { getLsnResult ->
            if (!getLsnResult.next()) {
                return null
            }
            return getLsnResult.getString(1)
        }
    }

    private fun forwardSlot(connection: Connection, slotName: String, beyondLsn: String) {
        val moveLsnQ = walMoveSlotQuery(slotName, beyondLsn)
        val stmt = connection.prepareStatement(moveLsnQ)
        stmt.executeQuery().use {  }
    }

    override fun stream(requests: Flow<SourceServiceOuterClass.StreamReq>): Flow<StreamRsp> {
        fun mkRsp(controlItem: Any): StreamRsp {
            val streamCtlRsp = StreamCtlRsp.newBuilder()
            when (controlItem) {
                is InitRsp -> streamCtlRsp.initRsp = controlItem
                is FixLsnRsp -> streamCtlRsp.fixLsnRsp = controlItem
                is CheckLsnRsp -> streamCtlRsp.checkLsnRsp = controlItem
                is StreamChangeRsp -> streamCtlRsp.streamChangeRsp = controlItem
                is RewindLsnRsp -> streamCtlRsp.rewindLsnRsp = controlItem
                else -> throw IllegalArgumentException("Unknown control item type: ${controlItem.javaClass}")
            }
            return StreamRsp.newBuilder()
                .setResult(RspUtil.resultOk)
                .setStreamCtlRsp(streamCtlRsp)
                .build()
        }

        fun mkBadRsp(error: String): StreamRsp =
            StreamRsp.newBuilder().setResult(RspUtil.resultError(error)).build()

        val clusterSource = StreamSource.newBuilder().setCluster(StreamSource.Cluster.getDefaultInstance())

        return flow {
            lateinit var clientId: String
            lateinit var connection: Connection

            fun genSlotName(clientId: String) = "slot_for_$clientId"

            requests.collect { req ->
                try {
                    when (req.streamCtlReq?.ctlReqCase) {
                        StreamCtlReq.CtlReqCase.INIT_REQ -> {
                            // STEP 1: this branch should initialize connection with database
                            // THIS STAGE IS NEVER SKIPPED BY CLIENT AFTER EACH gRPC REQUEST!
                            //
                            // This is the first control message that should be sent by client
                            // that established connection with service in order to
                            // service be able to establish connection with database
                            val initConnReq = req.streamCtlReq.initReq
                            clientId = initConnReq.clientId ?: UUID.randomUUID().toString()
                            // check spec and initialize connection (as in previous handles)
                            this@PostgresSource.ValidateSpec(initConnReq.jsonSettings)
                            connection = this@PostgresSource.connectToPostgreSQL(initConnReq.jsonSettings)
                            emit(mkRsp(InitRsp.newBuilder().setClientId(clientId).build()))
                        }
                        StreamCtlReq.CtlReqCase.FIX_LSN_REQ -> {
                            // STEP 2: the next thing client will want is to fix LSN position
                            // This can be skipped if Client have already done this step and
                            // didn't send request for REWIND_LSN_REQ, or client received
                            // LostRequestedLsnRsp: on such cases client may request to fix LSN again
                            val fixLsnReq = req.streamCtlReq.fixLsnReq
                            when (fixLsnReq.streamSource.sourceCase) {
                                StreamSource.SourceCase.CLUSTER -> {
                                    // OK
                                }
                                StreamSource.SourceCase.TABLE,
                                StreamSource.SourceCase.NAMESPACE,
                                null, StreamSource.SourceCase.SOURCE_NOT_SET -> {
                                    throw DtExtensionException("Only whole cluster cursor is acceptable")
                                }
                            }

                            val slotName = genSlotName(clientId)
                            var lsnString: String? = getCurrentLsn(connection, slotName)
                            if (lsnString == null) {
                                // otherwise create it
                                val lockLsnQ = walLockLsnQuery((slotName))
                                val stmt = connection.prepareStatement(lockLsnQ)
                                stmt.executeQuery().use { lockResult ->
                                    if (lockResult.next()) {
                                        lsnString = lockResult.getString(1)
                                    }
                                }
                            }
                            if (lsnString == null) {
                                emit(mkBadRsp("cannot fix LSN: no LSN point provided"))
                                return@collect
                            }

                            // after creating slot, save it's LSN in the format of column
                            val slotLsn = ColumnValue.newBuilder().setString(lsnString).build()

                            // you can identify allocated resources here, for instance,
                            // you may save slot name that was just created
                            // but this is not needed in current implementation
                            val replicationState = ByteString.copyFromUtf8("Hello, world!")
                            emit(
                                mkRsp(
                                    FixLsnRsp.newBuilder()
                                        .setLsn(Lsn.newBuilder()
                                            .setReplicationState(replicationState)
                                            .setStreamSource(clusterSource)
                                            .setLsnValue(slotLsn)
                                        )
                                        .build()
                                )
                            )
                        }
                        StreamCtlReq.CtlReqCase.CHECK_LSN_REQ -> {
                            // normally, we should check if LSN still here, but PostgreSQL guarantees
                            // LSN persistance if user hasn't moves slot of replication.
                            val forLsn = req.streamCtlReq.checkLsnReq.lsn.lsnValue.string
                            val slotName = genSlotName(clientId)
                            val currentLsn = this@PostgresSource.getCurrentLsn(connection, slotName)
                            if (currentLsn != null) {
                                emit(mkBadRsp("Slot has been lost because of no data: lsn=$forLsn"))
                                return@collect
                            }
                            // emit all ok
                            emit(mkRsp(CheckLsnRsp.newBuilder().setAlive(true).build()))
                        }
                        StreamCtlReq.CtlReqCase.REWIND_LSN_REQ -> {
                            val forLsn = req.streamCtlReq.rewindLsnReq.lsn
                            val slotName = genSlotName(clientId)
                            // first things first, check slot:
                            val currentLsn = this@PostgresSource.getCurrentLsn(connection, slotName)
                            if (currentLsn == null) {
                                // slot has already been removed, return OK
                                emit(mkRsp(RewindLsnRsp.newBuilder().build()))
                                return@collect
                            }
                            val deleteSlotQ = walDeleteSlotQuery(slotName)
                            val stmt = connection.prepareStatement(deleteSlotQ)
                            stmt.executeQuery().use { lockResult ->
                                if (!lockResult.next()) {
                                    emit(mkBadRsp("cannot remove LSN: empty result set"))
                                    return@collect
                                }
                            }
                            // emit OK otherwise
                            emit(mkRsp(RewindLsnRsp.newBuilder().build()))
                        }
                        StreamCtlReq.CtlReqCase.STREAM_CHANGE_REQ -> {
                            val changeReq = req.streamCtlReq.streamChangeReq
                            val forLsn = changeReq.lsn.lsnValue.string
                            val slotName = genSlotName(clientId)

                            val currentLsn = this@PostgresSource.getCurrentLsn(connection, slotName)
                            if (currentLsn == null) {
                                emit(mkBadRsp("Slot has been lost"))
                                return@collect
                            }
                            // rewind LSN to committed position by client
                            forwardSlot(connection, slotName, forLsn)
                            // peak next change(changes) from stream
                            val peakWalChangesReq = walPeakChangesQuery(slotName, limit=1)
                            val stmt = connection.prepareStatement(peakWalChangesReq)
                            stmt.executeQuery().use { getChangesResult ->
                                // expect only single result
                                if (getChangesResult.next()) {
                                    val lsn = getChangesResult.getString(1)
                                    val data = getChangesResult.getString(2)

                                    val wal2json = Klaxon().parse<Wal2JsonMessage>(data)
                                        ?: throw DtExtensionException("Wrong format of wal2json message")

                                    wal2json.getDataChangeItems().forEach {
                                        emit(
                                            mkRsp(
                                                StreamChangeRsp.newBuilder()
                                                    .setChangeItem(
                                                        ChangeItem.newBuilder().setDataChangeItem(it)
                                                    )
                                                    .build()
                                            )
                                        )
                                    }

                                    // don't forget to send final message
                                    val nextLsnCol = ColumnValue.newBuilder().setString(wal2json.nextlsn).build()
                                    emit(mkRsp(
                                        StreamChangeRsp.newBuilder()
                                            .setCheckpoint(StreamChangeRsp.CheckPoint.newBuilder()
                                                .setUnixCommitTime(wal2json.unixTimestamp())
                                                .setLsn(
                                                    Lsn.newBuilder()
                                                        .setStreamSource(clusterSource)
                                                        .setLsnValue(nextLsnCol)
                                                )
                                            ) .build()
                                    )
                                    )
                                    return@collect
                                }
                                // here also don't forget to send final message: just return the same LSN
                                emit(mkRsp(
                                    StreamChangeRsp.newBuilder()
                                        .setCheckpoint(StreamChangeRsp.CheckPoint.newBuilder()
                                            .setLsn(changeReq.lsn)
                                        ) .build()
                                )
                                )
                            }
                        }
                        null, StreamCtlReq.CtlReqCase.CTLREQ_NOT_SET ->
                            emit(mkBadRsp("no control item request sent"))
                    }
                } catch (e: java.lang.Exception) {
                    emit(mkBadRsp("exception occured: ${e.message}, stack trace: ${e.stackTraceToString()}"))
                }
            }
        }
    }
}