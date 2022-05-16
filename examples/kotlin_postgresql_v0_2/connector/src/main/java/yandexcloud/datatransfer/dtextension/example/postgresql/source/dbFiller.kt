package yandexcloud.datatransfer.dtextension.example.postgresql.source

import kotlinx.cli.*
import java.lang.Integer.min
import java.sql.Connection
import java.sql.DriverManager

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
            return result.getInt(1).toString()
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
        return result.getBoolean(1)
    }
}

fun checkTableIsEmpty(connection: Connection, table: String) : Boolean {
    val query = """SELECT count(*) FROM "public"."$table";"""
    connection.prepareStatement(query).use {
        val result = it.executeQuery()
        if (!result.next()) {
            return true
        }
        return result.getInt(1) == 0
    }
}

fun getMaxId(connection: Connection, table: String) : Int {
    val query = "SELECT max(id) FROM $table;"
    val stmt = connection.prepareStatement(query)
    // stmt.setString(1, table)
    val result = stmt.executeQuery()
    if (result.next()) {
        return result.getInt(1)
    }
    return 0
}

fun createTable(connection: Connection, table: String) {
    val query = """
        CREATE TABLE IF NOT EXISTS "public"."$table" (
            id bigint primary key,
            mono_col bigint,
            mono_date_col timestamp with time zone,
            uniform_col double precision,
            expo_col double precision,
            minus_expo_col double precision,
            multimodal_expo_col double precision,
            normal_col double precision,
            title text,
            description text,
            padding text,
            integer_constant int
        );
        
        CREATE TABLE IF NOT EXISTS registry (
            id integer primary key,
            ts timestamp,
            amountOfInserts int
        );
        
        DROP FUNCTION IF EXISTS ts_registrer CASCADE;
        
        CREATE FUNCTION ts_registrer()
            RETURNS trigger AS $$
        BEGIN
          INSERT INTO registry (id, ts, amountOfInserts) VALUES (NEW.id, now(), 1)
          ON CONFLICT (id) DO UPDATE
            SET amountOfInserts = excluded.amountOfInserts + 1;
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER ts_registrer_trigger
            BEFORE INSERT ON example
            FOR EACH ROW
            EXECUTE PROCEDURE ts_registrer();
    """.trimIndent()
    val stmt = connection.prepareStatement(query)
    stmt.execute()
}

fun appendTable(connection: Connection, table: String, from: Int, to: Int) {
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
    stmt.setInt(1, from)
    stmt.setInt(2, to)
    stmt.execute()
}

fun main(args : Array<String>) {
    val parser = ArgParser("PostgreSQL JDBC Database Filler")
    val jdbcString by parser.option(ArgType.String, shortName = "j", fullName = "jdbc", description = "JDBC PostgreSQL connection string").required()
    val user by parser.option(ArgType.String, shortName = "u", fullName = "user", description = "Username").required()
    val password by parser.option(ArgType.String, shortName = "p", fullName = "pwd", description = "Password").required()
    val table by parser.option(ArgType.String, shortName = "t", fullName = "tbl", description = "Table name in public schema").required()


    // default values are for replication (inserts sinble value every second
    val deltaCount by parser.option(ArgType.Int, shortName = "n", description = "Amount of inserted values per action").default(1)
    val maxLines by parser.option(ArgType.Int, shortName = "M", description = "Maximum lines in table").default(999999999)
    val sleepMs by parser.option(ArgType.Int, shortName = "s", description = "Sleep in milliseconds between inserts").default(1000)
    parser.parse(args)

    val delta = deltaCount - 1

    val connection  = DriverManager.getConnection(jdbcString, user, password)

    while (true) {
        val tableSize = getTableSize(connection, table)
        val rowCount = getTableRowsCount(connection, table)
        println("Estimation of table: $tableSize size, $rowCount rows")

        if (!checkTableExists(connection, table)) {
            createTable(connection, table)
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

        if (sleepMs > 0L) {
            Thread.sleep(sleepMs.toLong())
        }
    }
    val tableSize = getTableSize(connection, table)
    val rowCount = getTableRowsCount(connection, table)
    println("Table generation done! Estimation of table: $tableSize size, $rowCount rows")
}