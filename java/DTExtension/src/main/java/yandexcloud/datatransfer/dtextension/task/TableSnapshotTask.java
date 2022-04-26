package yandexcloud.datatransfer.dtextension.task;

import yandexcloud.datatransfer.dtextension.cdc.Table;

/**
 * TableSnapshotTask is nonsplittable task for snapshotting single table.
 * The table is meaning from classic RDBMS, for other systems it can be various things:
 * 1. Classic RDBMS, e.g. PostgreSQL, MySQL, ...: table is table, rows are rows.
 * 2. File systems, S3 storages: single file. Rows of this 'table' are the rows of the file. The path of the file
 * it is namespace of the table (e.g. schema, database in RDBMS), the name of the file is the name of the table
 * 3. NoSQL, graph/document databases, e.g. MongoDB: a collection in database. The row is the document.
 */
public interface TableSnapshotTask extends SnapshotTask {
    /**
     * @return the table which is extracted as events by this task
     */
    Table getTable();
}
