package yandexcloud.datatransfer.dtextension.task;

import yandexcloud.datatransfer.dtextension.cdc.Database;

/**
 * Some types of databases allow you to extract changes from single database.
 */
public interface DatabaseStreamTask extends StreamTask {

    /**
     * @return the database whose change data capture is performed
     */
    Database getDatabase();
}
