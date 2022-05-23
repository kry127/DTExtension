package yandexcloud.datatransfer.dtextension.example.postgresql.source

import kotlinx.cli.*
import java.lang.Integer.min
import java.sql.Connection
import java.sql.DriverManager
import kotlin.random.Random
import kotlin.random.nextInt

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

fun getMinId(connection: Connection, table: String) : Int {
    val query = "SELECT min(id) FROM $table;"
    val stmt = connection.prepareStatement(query)
    val result = stmt.executeQuery()
    if (result.next()) {
        return result.getInt(1)
    }
    return 0
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

fun updateDocument(connection: Connection, table: String, id: Int) {
    val query = """
        UPDATE $table
        SET -- 264B per line
            uniform_col = 2000 * random() - 1000, -- 8B, E[x] == 0
            expo_col = -log(1 - random()) * 1000 - 1000, -- 8B, E[x] = 0
            minus_expo_col = -(-log(1 - random()) * 1000 - 1000), -- 8B, E[x] = 0
            multimodal_expo_col = -(-log(1 - random()) * 1000 - 1000) + (-log(1 - random()) * 1000 - 1000), -- 8B, E[x] = 0
            normal_col = (random() + random() + random() + random() + random() + random() + random() + random() + random() + random() + random() + random() + random() + random() + random() + random()
                 - 8) * 125, -- 8B, E[x] = 0
            title = md5(random()::text) || md5(random()::text), -- 64B
            description = md5(random()::text) || md5(random()::text) || md5(random()::text) || md5(random()::text), -- 128B
            padding = 'data_pad', -- 8B
            integer_constant = 1 -- 8B
        WHERE id=?;
    """.trimIndent()
    val stmt = connection.prepareStatement(query)
    stmt.setInt(1, id)
    stmt.execute()
}

fun deleteDocument(connection: Connection, table: String, id: Int) {
    val query = """DELETE FROM $table WHERE id=?;""".trimIndent()
    val stmt = connection.prepareStatement(query)
    stmt.setInt(1, id)
    stmt.execute()
}

fun main(args : Array<String>) {
    val parser = ArgParser("PostgreSQL JDBC Database Filler")
    val jdbcString by parser.option(ArgType.String, shortName = "j", fullName = "jdbc", description = "JDBC PostgreSQL connection string").required()
    val user by parser.option(ArgType.String, shortName = "u", fullName = "user", description = "Username").required()
    val password by parser.option(ArgType.String, shortName = "p", fullName = "pwd", description = "Password").required()
    val table by parser.option(ArgType.String, shortName = "t", fullName = "tbl", description = "Table name in public schema").required()

    val updates by parser.option(ArgType.Boolean, fullName = "updates", description = "Generate updates").default(false)
    val deletes by parser.option(ArgType.Boolean, fullName = "deletes", description = "Generate deletes").default(false)


    // default values are for replication (inserts sinble value every second)
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
        val isEmpty = checkTableIsEmpty(connection, table)
        var opType = Random.nextInt(1 + if(updates){1}else{0} + if(deletes){1}else{0})
        if (isEmpty) {
            opType = 0 // insert
        }
        if (opType == 1 && !updates) {
            // if updates are disabled, but op type is not insert, then, it is delete
            opType = 2
        }
        when (opType) {
            0 -> {
                // insert delta
                val maxId = getMaxId(connection, table)
                val fromId = if (isEmpty) {0} else {maxId + 1}
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
            }
            1 -> {
                // update
                val fromId = getMinId(connection, table)
                val toId = getMaxId(connection, table)
                val id = Random.nextInt(fromId..toId)
                println("Updating document $id")
                updateDocument(connection, table, id)
            }
            2 -> {
                // delete
                val fromId = getMinId(connection, table)
                println("Deleting document $fromId")
                deleteDocument(connection, table, fromId)
            }
        }

        if (sleepMs > 0L) {
            Thread.sleep(sleepMs.toLong())
        }
    }
    val tableSize = getTableSize(connection, table)
    val rowCount = getTableRowsCount(connection, table)
    println("Table generation done! Estimation of table: $tableSize size, $rowCount rows")
}