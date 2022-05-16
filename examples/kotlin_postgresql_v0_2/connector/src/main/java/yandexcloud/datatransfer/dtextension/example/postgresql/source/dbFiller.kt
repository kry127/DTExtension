package yandexcloud.datatransfer.dtextension.example.postgresql.source

import java.lang.Long.min
import java.sql.Connection
import java.sql.DriverManager
import kotlin.math.max

fun getTableSize(connection: Connection, table: String) : String {
    try {
        val query = """
        SELECT pg_size_pretty(pg_total_relation_size('"public"."$table"'));
    """.trimIndent()
        val stmt = connection.prepareStatement(query)
        val result = stmt.executeQuery()
        if (result.next()) {
            return result.getString(1)
        }
    } catch (ignored : java.lang.Exception) {

    }
    return "Unknown"
}

fun getTableRowsCount(connection: Connection, table: String) : String {
    try {
        val query = """
        SELECT COUNT(*) FROM "public"."$table";
    """.trimIndent()
        val stmt = connection.prepareStatement(query)
        val result = stmt.executeQuery()
        if (result.next()) {
            return result.getLong(1).toString()
        }
    } catch (ignored : java.lang.Exception) {

    }
    return "Unknown"
}

fun checkTableExists(connection: Connection, table: String) : Boolean {
    val query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
                    WHERE  table_schema = 'public'
                    AND    table_name   = '$table'
            );""".trimIndent()
    connection.prepareStatement(query).use {
        val result = it.executeQuery()
        if (!result.next()) {
            return false
        }
        return true
    }
}

fun checkTableIsEmpty(connection: Connection, table: String) : Boolean {
    val query = """SELECT count(*) FROM "public"."$table";"""
    connection.prepareStatement(query).use {
        val result = it.executeQuery()
        if (!result.next()) {
            return true
        }
        return result.getLong(1) == 0L
    }
}

fun getMaxId(connection: Connection, table: String) : Long {
    val query = "SELECT max(id) FROM $table;"
    val stmt = connection.prepareStatement(query)
    // stmt.setString(1, table)
    val result = stmt.executeQuery()
    if (result.next()) {
        return result.getLong(1)
    }
    return 0
}

fun createTable(connection: Connection, table: String, to: Long) {
    val query = """
        CREATE TABLE $table AS
        SELECT -- 264B per line
            i AS id, -- 4B
            i / 10 as mono_col, -- 4B
            now() + i * interval '1 second' as mono_date_col, -- 8B
            2000 * random() - 1000 as uniform_col, -- 8B, E[x] == 0
            -log(1 - random()) * 1000 - 1000 as expo_col, -- 8B, E[x] = 0
            -(-log(1 - random()) * 1000 - 1000) as minus_expo_col, -- 8B, E[x] = 0
            -(-log(1 - random()) * 1000 - 1000) + (-log(1 - random()) * 1000 - 1000)
              as multimodal_expo_col, -- 8B, E[x] = 0
            (random() + random() + random() + random() + random() + random() + random() + random() + random() + random() + random() + random() + random() + random() + random() + random()
                 - 8) * 125 as normal_col, -- 8B, E[x] = 0
            md5(random()::text) || md5(random()::text) as title, -- 64B
            md5(random()::text) || md5(random()::text) || md5(random()::text) || md5(random()::text) as description, -- 128B
            'data_pad' as padding, -- 8B
            1 as integer_constant -- 8B
        FROM generate_series(0,?) as i;
    """.trimIndent()
    val stmt = connection.prepareStatement(query)
    stmt.setLong(1, to)
    stmt.execute()
}


fun appendTable(connection: Connection, table: String, from: Long, to: Long) {
    val query = """
        INSERT INTO $table
        SELECT -- 264B per line
            i AS id, -- 4B
            i / 10 as mono_col, -- 4B
            now() + i * interval '1 second' as mono_date_col, -- 8B
            2000 * random() - 1000 as uniform_col, -- 8B, E[x] == 0
            -log(1 - random()) * 1000 - 1000 as expo_col, -- 8B, E[x] = 0
            -(-log(1 - random()) * 1000 - 1000) as minus_expo_col, -- 8B, E[x] = 0
            -(-log(1 - random()) * 1000 - 1000) + (-log(1 - random()) * 1000 - 1000)
              as multimodal_expo_col, -- 8B, E[x] = 0
            (random() + random() + random() + random() + random() + random() + random() + random() + random() + random() + random() + random() + random() + random() + random() + random()
                 - 8) * 125 as normal_col, -- 8B, E[x] = 0
            md5(random()::text) || md5(random()::text) as title, -- 64B
            md5(random()::text) || md5(random()::text) || md5(random()::text) || md5(random()::text) as description, -- 128B
            'data_pad' as padding, -- 8B
            1 as integer_constant -- 8B
        FROM generate_series(?,?) as i;
    """.trimIndent()
    val stmt = connection.prepareStatement(query)
    stmt.setLong(1, from)
    stmt.setLong(2, to)
    stmt.execute()
}

fun main() {
    val connString = "jdbc:postgresql://rc1a-cajthro6i7y2crrx.mdb.yandexcloud.net:6432/db?&targetServerType=master&ssl=true&sslmode=verify-full"
    val user = "user"
    val password = "DTExtension"
    val table = "example"
    // for snapshot:
//    val delta = 10000L
//    val maxLines = 12500L
//    val sleepInMilliseconds = 0L
    // for replication:
    val delta = 0L
    val maxLines = 9999999999L
    val sleepInMilliseconds = 1000L
    val connection  = DriverManager.getConnection(connString, user, password)

    while (true) {
        val tableSize = getTableSize(connection, table)
        val rowCount = getTableRowsCount(connection, table)
        println("Estimation of table: $tableSize size, $rowCount rows")

        val exists = checkTableExists(connection, table)
        if (!exists) {
            val uptoId = delta - 1
            println("Generating new table [0, ${uptoId}]")
            createTable(connection, table, uptoId)
            continue
        }
        val maxId = getMaxId(connection, table)
        val fromId = if (checkTableIsEmpty(connection, table)) {0} else {maxId + 1}
        val uptoId = min(fromId + delta, maxLines - 1)
        if (fromId > uptoId) {
            println("Table already hit the cap")
            break
        }
        println("Appending delta [$fromId, $uptoId]")
        appendTable(connection, table, fromId, uptoId)
        if (uptoId == maxLines - 1) {
            break
        }

        if (sleepInMilliseconds > 0L) {
            java.lang.Thread.sleep(sleepInMilliseconds)
        }
    }
    val tableSize = getTableSize(connection, table)
    val rowCount = getTableRowsCount(connection, table)
    println("Table generation done! Estimation of table: $tableSize size, $rowCount rows")
}