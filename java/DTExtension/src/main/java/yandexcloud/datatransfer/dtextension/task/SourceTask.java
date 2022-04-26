package yandexcloud.datatransfer.dtextension.task;

import yandexcloud.datatransfer.dtextension.cdc.ChangeItem;
import yandexcloud.datatransfer.dtextension.cdc.SimpleChangeItem;

import java.util.function.Consumer;

/**
 * SourceTask is a task that extracts data from source database, file system, etc...
 * and emits data to the SinkTask
 */
public interface SourceTask extends Task {
    /**
     * This method emits change items from database to the target
     *
     * @param changeItemsConsumer a consumer of change item sequence
     */
    void emitChangeItems(Consumer<ChangeItem> changeItemsConsumer);
}
