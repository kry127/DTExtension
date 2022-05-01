package yandexcloud.datatransfer.dtextension.task;

import yandexcloud.datatransfer.dtextension.cdc.Table;

/**
 * A change data capture task for single table
 */
public interface TableStreamTask extends StreamTask {

    /**
     * @return the table whose change events are captured
     */
    Table getTable();
}
