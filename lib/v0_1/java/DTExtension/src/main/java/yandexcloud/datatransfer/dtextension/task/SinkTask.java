package yandexcloud.datatransfer.dtextension.task;

import yandexcloud.datatransfer.dtextension.cdc.ChangeItem;
import yandexcloud.datatransfer.dtextension.cdc.SimpleChangeItem;


/**
 * SinkTask is a task that writes data to target database, file system, etc...
 * emitted by SourceTask
 */
public interface SinkTask extends Task {
    /**
     * This method invoked when pack of change items should be written to destination
     *
     * @param changeItemsConsumer a consumer of change item sequence
     */
    void consumeChangeItems(ChangeItem changeItemsConsumer);
}
