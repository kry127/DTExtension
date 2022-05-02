package yandexcloud.datatransfer.dtextension.example.s3

import com.beust.klaxon.Klaxon
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import net.pwall.json.schema.JSONSchema
import yandexcloud.datatransfer.dtextension.v0_2.Common
import yandexcloud.datatransfer.dtextension.v0_2.Common.ColumnCursor
import yandexcloud.datatransfer.dtextension.v0_2.Common.Cursor
import yandexcloud.datatransfer.dtextension.v0_2.Data
import yandexcloud.datatransfer.dtextension.v0_2.Data.Column
import yandexcloud.datatransfer.dtextension.v0_2.Data.ColumnType
import yandexcloud.datatransfer.dtextension.v0_2.Data.ColumnValue
import yandexcloud.datatransfer.dtextension.v0_2.Data.Table
import yandexcloud.datatransfer.dtextension.v0_2.columnCursor
import yandexcloud.datatransfer.dtextension.v0_2.source.Control
import yandexcloud.datatransfer.dtextension.v0_2.source.Control.BeginSnapshotRsp
import yandexcloud.datatransfer.dtextension.v0_2.source.Control.CursorRsp
import yandexcloud.datatransfer.dtextension.v0_2.source.Control.DataItemRsp
import yandexcloud.datatransfer.dtextension.v0_2.source.Control.DoneSnapshotRsp
import yandexcloud.datatransfer.dtextension.v0_2.source.Control.InitConnectionRsp
import yandexcloud.datatransfer.dtextension.v0_2.source.Control.ReadControlItemRsp
import yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceGrpcKt
import yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceOuterClass
import yandexcloud.datatransfer.dtextension.v0_2.source.SourceServiceOuterClass.ReadRsp
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

val connector_id = "kry127.postgresql_example"

// TODO make part of library?
object RspUtil {
    val resultOk = Common.Result.newBuilder().setOk(true).build()
    fun resultError(error: String): Common.Result {
        return Common.Result.newBuilder().setError(error).build()
    }
}

class DtExtensionException(error: String) : Exception(error);

object PsqlQueries {
    private val pgSystemSchemas = listOf("pg_catalog", "information_schema")
    private val pgSystemTableNames = listOf("repl_mon", "pg_stat_statements")

    fun ParsePgType(pgType: String): Data.ColumnType {
        val pgPrefix = "pg:"
        val pgTypeTrim = if (pgType.startsWith(pgPrefix)) {
            pgType.substring(pgPrefix.length)
        } else pgType

        if (pgTypeTrim.startsWith("character") or pgTypeTrim.startsWith("character varying")) {
            return Data.ColumnType.COLUMN_TYPE_STRING
        }
        if (pgTypeTrim.startsWith("bit(") or pgTypeTrim.startsWith("bit varying(")) {
            return Data.ColumnType.COLUMN_TYPE_BINARY
        }
        when (pgType) {
            "timestamp without time zone", "timestamp with time zone", "time without time zone", "time with time zone", "date", "data" -> return Data.ColumnType.COLUMN_TYPE_ISO_TIME
            "uuid", "name", "text", "interval", "char", "abstime", "money"
            -> return Data.ColumnType.COLUMN_TYPE_STRING
            "boolean" -> return Data.ColumnType.COLUMN_TYPE_BOOL
            "bigint" -> return Data.ColumnType.COLUMN_TYPE_INT64
            "smallint" -> return Data.ColumnType.COLUMN_TYPE_INT32
            "integer" -> return Data.ColumnType.COLUMN_TYPE_INT32
            "numeric", "real", "double precision" -> return Data.ColumnType.COLUMN_TYPE_DOUBLE
            "bytea", "bit", "bit varying" -> return Data.ColumnType.COLUMN_TYPE_BINARY
            "json", "jsonb" -> return Data.ColumnType.COLUMN_TYPE_JSON
            "daterange", "int4range", "int8range", "numrange", "point", "tsrange",
            "tstzrange", "xml", "inet", "cidr", "macaddr", "oid" ->
                return Data.ColumnType.COLUMN_TYPE_JSON
            else -> return Data.ColumnType.COLUMN_TYPE_JSON
        }
    }

    fun ListTablesQuery(schema: String): String {
        val schemaCondition = if (schema == "*") {
            "AND ns.nspname = \$1"
        } else {
            "AND ns.nspname NOT IN (${pgSystemTableNames.joinToString { it }})"
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
    AND c.relname NOT IN (${pgSystemSchemas.joinToString { it }})
    AND c.relkind = 'r'
        """
    }

    fun ListTableSchemaQuery(): String {
        return """
SELECT column_name, data_type, column_default, is_nullable
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = $1 AND table_name = $2;
        """
    }

    fun QueryTableKey(): String {
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
            ns.nspname = $1 AND c.relname = $2
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

data class PostgreSQLParameters(
    val pg_jdbc_connection_string: String,
)

class PostgreSQL : SourceServiceGrpcKt.SourceServiceCoroutineImplBase() {
    private val specificationPath = "/spec.json";

    private fun ValidateSpec(jsonSpec: String) {
        val specPath = javaClass.getResource(specificationPath)
            ?: throw DtExtensionException("Spec file not found")
        val specSchema = JSONSchema.parseFile(specPath.path)
        val output = specSchema.validateBasic(jsonSpec)
        if (output.errors != null) {
            val err = output.errors?.map { it.instanceLocation + "(" + it.keywordLocation + "):" + it.error }
                ?.joinToString(separator = "\n") ?: ""
            throw DtExtensionException(err)
        }
        // TODO add extra validation if needed
    }

    private fun ConnectToPostgreSQL(jsonSpec: String): Connection {
        val parameters = Klaxon().parse<PostgreSQLParameters>(jsonSpec)

        val jdbcUrl = parameters?.pg_jdbc_connection_string
            ?: throw DtExtensionException("JDBC PostgreSQL connection URL cannot be empty")

        return DriverManager
            .getConnection(jdbcUrl, "postgres", "postgres")
    }

    private fun SchemaQuery(connection: Connection, namespace: String, name: String): Data.Schema {
        val schemaQuery = connection.prepareStatement(PsqlQueries.ListTableSchemaQuery())
        schemaQuery.setString(1, namespace)
        schemaQuery.setString(2, name)
        val schemaResult = schemaQuery.executeQuery()

        val columns = mutableMapOf<String, Column.Builder>()
        while (schemaResult.next()) {
            val columnName = schemaResult.getString(1)
            val dataType = schemaResult.getString(2)

            columns[columnName] = Column.newBuilder()
                .setName(columnName)
                .setType(PsqlQueries.ParsePgType(dataType))
                .setOriginalType(
                    Column.OriginalType.newBuilder()
                        .setConnectorId(connector_id)
                        .setTypeName(dataType)
                        .build()
                )
        }
        schemaResult.close()


        val pkeyQuery = connection.prepareStatement(PsqlQueries.QueryTableKey())
        pkeyQuery.setString(1, namespace)
        pkeyQuery.setString(2, name)
        val pkeyQueryResult = pkeyQuery.executeQuery()
        while (pkeyQueryResult.next()) {
            val columnName = pkeyQueryResult.getString(3)
            columns[columnName]?.key = true
        }
        pkeyQueryResult.close()

        val schema = Data.Schema.newBuilder()
            .addAllColumns(columns.map { it.value.build() })
            .build()
        return schema
    }

    private fun DeltaTableQuery(
        cursor: Cursor, namespace: String, name: String,
        schema: Data.Schema, limit: Int
    ): String {
        val colCursor = cursor.columnCursor ?: throw DtExtensionException("Only column cursor is supported")
        val columnList = schema.columnsList.joinToString { "`${it.name}`" }
        val leftWhere = colCursor.dataRange.from?.let {
            if (colCursor.dataRange.excludeFrom) {
                "AND `${colCursor.column.name}` > ${ColumnValueAsSqlString(it)}"
            } else {
                "AND `${colCursor.column.name}` >= ${ColumnValueAsSqlString(it)}"
            }
        } ?: ""
        val rightWhere = colCursor.dataRange.to?.let {
            if (colCursor.dataRange.excludeTo) {
                "AND `${colCursor.column.name}` < ${ColumnValueAsSqlString(it)}"
            } else {
                "AND `${colCursor.column.name}` <= ${ColumnValueAsSqlString(it)}"
            }
        } ?: ""
        val query = """
            SELECT $columnList
            FROM `$namespace`.`$name`
            WHERE 1=1
            $leftWhere
            $rightWhere
            ORDER BY ${colCursor.column.name} ${
            if (colCursor.descending) {
                "desc"
            } else ""
        }
            LIMIT $limit
            """
        return query
    }

    private fun GetColumnValue(result: ResultSet, id: Int, columnType: ColumnType): ColumnValue {
        val columnBuilder = ColumnValue.newBuilder()
        when (columnType) {
            Data.ColumnType.COLUMN_TYPE_BOOL -> return columnBuilder.setBool(result.getBoolean(id)).build()
            Data.ColumnType.COLUMN_TYPE_INT32 -> return columnBuilder.setInt32(result.getInt(id)).build()
            Data.ColumnType.COLUMN_TYPE_INT64 -> return columnBuilder.setInt64(result.getLong(id)).build()
            Data.ColumnType.COLUMN_TYPE_UINT32 -> return columnBuilder.setUint32(result.getInt(id)).build()
            Data.ColumnType.COLUMN_TYPE_UINT64 -> return columnBuilder.setUint64(result.getLong(id)).build()
            Data.ColumnType.COLUMN_TYPE_FLOAT -> return columnBuilder.setFloat(result.getFloat(id)).build()
            Data.ColumnType.COLUMN_TYPE_DOUBLE -> return columnBuilder.setDouble(result.getDouble(id)).build()
            Data.ColumnType.COLUMN_TYPE_JSON -> return columnBuilder.setJson(result.getString(id)).build()
            Data.ColumnType.COLUMN_TYPE_DECIMAL -> {
                val bigDecimal = result.getBigDecimal(id)
                return columnBuilder.setDecimal(
                    Data.Decimal.newBuilder()
                        .setAsString(bigDecimal.toString())
                        .setPrecision(bigDecimal.precision())
                        .setScale(bigDecimal.scale())
                        .build()
                ).build()
            }
            Data.ColumnType.COLUMN_TYPE_BIG_DECIMAL,
            Data.ColumnType.COLUMN_TYPE_BIG_INTEGER -> {
                return columnBuilder.setBigDecimal(result.getBigDecimal(id).toString()).build()
            }
            Data.ColumnType.COLUMN_TYPE_UNIX_TIME -> return columnBuilder.setUnixTime(result.getTimestamp(id).time)
                .build()
            Data.ColumnType.COLUMN_TYPE_ISO_TIME -> {
                // https://mkyong.com/java/how-to-get-current-timestamps-in-java/
                val date = result.getTimestamp(id)
                val isoFormat = date.toInstant().toString()
                return columnBuilder.setString(isoFormat).build()
            }
            Data.ColumnType.COLUMN_TYPE_STRING -> return columnBuilder.setString(result.getString(id)).build()
            Data.ColumnType.COLUMN_TYPE_BINARY,
            Data.ColumnType.COLUMN_TYPE_UNSPECIFIED,
            Data.ColumnType.UNRECOGNIZED -> {
                return columnBuilder.setBinary(ByteString.readFrom(result.getBinaryStream(id))).build()
            }
        }
    }

    private fun ColumnValueAsSqlString(columnValue: ColumnValue): String {
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
                throw DtExtensionException("No data to convert ty SQL representation")
        }
    }

    private fun GetAscendingCursor(
        connection: Connection,
        namespace: String,
        name: String,
        column: Column,
        wholePrimaryKey: Boolean = false
    ): Cursor {
        val query = "SELECT max(`$3`) FROM `$1`.`$2`"
        val stmt = connection.prepareStatement(query)
        stmt.setString(1, namespace)
        stmt.setString(2, name)
        stmt.setString(3, column.name)
        val result = stmt.executeQuery()

        if (!result.next()) {
            throw DtExtensionException("Cannot get min/max for column")
        }
        val rangeMax = this.GetColumnValue(result, 1, column.type)
        return Cursor.newBuilder().setColumnCursor(
            Common.ColumnCursor.newBuilder()
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
    private fun GetTableDeltaAsPlainRow(
        connection: Connection, cursor: Common.Cursor, namespace: String, name: String,
        schema: Data.Schema, limit: Int
    ): List<Data.PlainRow> {
        val query = DeltaTableQuery(cursor, namespace, name, schema, limit)
        val stmt = connection.prepareStatement(query)
        val result = stmt.executeQuery()

        val changeItemList = mutableListOf<Data.PlainRow>()
        while (result.next()) {
            val rowBuilder = Data.PlainRow.newBuilder()
            for (id in 0 until result.metaData.columnCount) {
                val value = GetColumnValue(result, id, schema.columnsList.get(id).type)
                rowBuilder.addValues(value)
            }
            changeItemList.add(rowBuilder.build())
        }
        result.close()
        return changeItemList
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
            val connection = this.ConnectToPostgreSQL(request.jsonSettings)

            val query = connection
                .prepareStatement(PsqlQueries.ListTablesQuery("*"))
            query.setString(1, "*")

            val result = query.executeQuery()

            val tables = mutableListOf<Table>()

            while (result.next()) {
                val namespace = result.getString(1)
                val name = result.getString(2)

                // retrieve schema for each table
                val schema = this.SchemaQuery(connection, namespace, name)
                tables.add(
                    Table.newBuilder()
                        .setNamespace(Data.Namespace.newBuilder().setNamespace(name))
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

    override fun read(requests: Flow<SourceServiceOuterClass.ReadReq>): Flow<SourceServiceOuterClass.ReadRsp> {
        fun mkRsp(cursor: Cursor, controlItem: Any): ReadRsp {
            val readControlItemBuilder = ReadControlItemRsp.newBuilder()
            when (controlItem) {
                is InitConnectionRsp -> readControlItemBuilder.initConnectionRsp = controlItem
                is CursorRsp -> readControlItemBuilder.cursorRsp = controlItem
                is BeginSnapshotRsp -> readControlItemBuilder.beginSnapshotRsp = controlItem
                is DataItemRsp -> readControlItemBuilder.dataItemRsp = controlItem
                is DoneSnapshotRsp -> readControlItemBuilder.doneSnapshotRsp = controlItem
                else -> throw IllegalArgumentException("Unknown control item type: ${controlItem.javaClass}")
            }
            return ReadRsp.newBuilder()
                .setResult(RspUtil.resultOk)
                .setCursor(cursor)
                .setControlItemRsp(readControlItemBuilder.build())
                .build()
        }

        fun mkBadRsp(cursor: Cursor, error: String): ReadRsp =
            ReadRsp.newBuilder().setCursor(cursor).setResult(RspUtil.resultError(error)).build()

        return flow {
            // INFO: this is the only stateful resource per-gRPC call: connection to database.
            // It is initialized when INIT_CONNECTION_REQ message with full endpoint specification comes
            lateinit var connection: Connection;

            requests.collect { req ->
                val table = req.table
                val namespace = table.namespace.namespace
                val name = table.name
                val cursor = req.cursor
                try {
                    when (req.controlItemReq?.controlItemReqCase) {
                        Control.ReadControlItemReq.ControlItemReqCase.INIT_CONNECTION_REQ -> {
                            // STEP 1: this branch should initialize connection with database
                            // THIS STAGE IS NEVER SKIPPED BY CLIENT AFTER EACH gRPC REQUEST!
                            //
                            // This is the first control message that should be sent by client
                            // that established connection with service in order to
                            // service be able to establish connection with database
                            val initConnReq = req.controlItemReq.initConnectionReq
                            // check spec and initialize connection (as in previous handles
                            this@PostgreSQL.ValidateSpec(initConnReq.jsonSettings)
                            connection = this@PostgreSQL.ConnectToPostgreSQL(initConnReq.jsonSettings)
                            emit(mkRsp(cursor, InitConnectionRsp.getDefaultInstance()))
                        }
                        Control.ReadControlItemReq.ControlItemReqCase.CURSOR_REQ -> {
                            // STEP 2: then client should know, how to request ranges of data,
                            // that's why you should provide him cursor
                            // NOTE: client can skip this message after reconnect if it already has some cursor
                            //
                            // note, that expected cursor interval has no minimum value, because
                            // minimum value of interval is excluded and interpreted as last
                            // commited
                            // and selection of pieces is happened as select statement
                            val schema = this@PostgreSQL.SchemaQuery(connection, namespace, name)

                            val keyCount = schema.columnsList.count { it.key }
                            for (col in schema.columnsList) {
                                if (col.key) {
                                    val newCursor = GetAscendingCursor(
                                        connection,
                                        namespace,
                                        name,
                                        col,
                                        // if this column is a single key: exclude lower bound of cursor
                                        keyCount == 1
                                    )
                                    emit(mkRsp(newCursor, CursorRsp.getDefaultInstance()))
                                    break
                                }
                            }
                        }
                        Control.ReadControlItemReq.ControlItemReqCase.BEGIN_SNAPSHOT_REQ -> {
                            // STEP 3: this is just a notification when uploading of the table is starting
                            // NOTE: client can skip this step if it already began to upload table, but reconnection happened
                            val state = "Hello, world!"
                            print("Setting begin snapshot state: $state")
                            val controlItem =
                                BeginSnapshotRsp.newBuilder()
                                    // you may set here additional state that will be restored on DONE_SNAPSHOT_REQ
                                    .setSnapshotState(ByteString.copyFromUtf8(state))
                                    .build()
                            emit(mkRsp(cursor, controlItem))
                        }
                        Control.ReadControlItemReq.ControlItemReqCase.DATA_ITEM_REQ -> {
                            // STEP 4: the main routine
                            // Client would like to request corresponding lines for table
                            //
                            // Note, that you may emit here more than one data change item:
                            // that is increase efficiency of using network. For example, you may
                            // transmit 1000 items back to client. You can also pack your change
                            // items in parquet format to be even more efficient
                            val schema = this@PostgreSQL.SchemaQuery(connection, namespace, name)
                            val changeItems = this@PostgreSQL.GetTableDeltaAsPlainRow(
                                connection, cursor, namespace, name, schema, 1000
                            )
                            val columnCursor = cursor.columnCursor
                                ?: throw DtExtensionException("Only column cursors are supported")

                            val colCursorId = schema.columnsList.indexOfFirst { it.name == columnCursor.column.name }
                            if (colCursorId == -1) throw DtExtensionException("Column cursor not found in schema")

                            changeItems.forEach {
                                val controlItem =
                                    DataItemRsp.newBuilder().setChangeItem(
                                        Data.ChangeItem.newBuilder().setDataChangeItem(
                                            Data.DataChangeItem.newBuilder()
                                                .setSchema(schema)
                                                .setOpType(Data.OpType.OP_TYPE_INSERT)
                                                .setData(it)
                                        )
                                    )
                                val cursorKey = controlItem.changeItem.dataChangeItem.data.getValues(colCursorId)
                                val newDataRange = Common.DataRange.newBuilder(columnCursor.dataRange)
                                    .let {
                                        if (columnCursor.descending) {
                                            it.setTo(cursorKey)
                                        } else {
                                            it.setFrom(cursorKey)
                                        }
                                    }
                                    .build()
                                val newCursor = Cursor.newBuilder(cursor)
                                    .setColumnCursor(
                                        ColumnCursor.newBuilder(columnCursor)
                                            .setDataRange(newDataRange)
                                            .build()
                                    )
                                    .build()
                                emit(mkRsp(newCursor, controlItem))
                            }

                        }
                        Control.ReadControlItemReq.ControlItemReqCase.DONE_SNAPSHOT_REQ -> {
                            // STEP 5: when upload of the table is over, you will be notified by client
                            // You can clean resources if you created some in BEGIN_SNAPSHOT_REQ
                            val doneSnapshotReq = req.controlItemReq.doneSnapshotReq
                            // If you saved some state during INIT_SNAPSHOT_REQ, you can use it here.
                            // Motivation why you should do it like this, but not with local variable
                            // is reconnections. State is represented by request, not by the state of the program
                            print("Restored snapshot state by client: ${doneSnapshotReq.snapshotState.toStringUtf8()}")
                            emit(
                                mkRsp(cursor, DoneSnapshotRsp.getDefaultInstance())
                            )
                        }
                        null, Control.ReadControlItemReq.ControlItemReqCase.CONTROLITEMREQ_NOT_SET ->
                            emit(mkBadRsp(cursor, "no control item response"))
                    }
                } catch (e: java.lang.Exception) {
                    emit(mkBadRsp(cursor, "exception occured: ${e.message}"))
                }
            }
        }
    }

    override fun stream(requests: Flow<SourceServiceOuterClass.StreamReq>): Flow<SourceServiceOuterClass.StreamRsp> {
        return super.stream(requests)
    }
}