package yandexcloud.datatransfer.dtextension.task;

import yandexcloud.datatransfer.dtextension.cdc.ChangeItem;
import yandexcloud.datatransfer.dtextension.cdc.SimpleChangeItem;

import java.util.function.Consumer;

/**
 * StreamTask is a task that extracts data from source database, file system, etc...
 * and emits data to the SinkTask.
 * The difference from SnapshotTask is in the mode of usage by the service.
 * Expected, that SnapshotTask is a finite stream of events, while StreamTask
 * is infinite stream of events.
 * <p>
 * Events are generated in default format. Other formats are also applicable
 */
public interface StreamTask {
    /**
     * This method emits change items from database to the target
     *
     * @param changeItemsConsumer a consumer of change item sequence
     */
    void emitChangeItems(Consumer<ChangeItem> changeItemsConsumer);
}
