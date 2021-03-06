package yandexcloud.datatransfer.dtextension.source;

import yandexcloud.datatransfer.dtextension.guaranties.SourceTaskFailoverType;
import yandexcloud.datatransfer.dtextension.task.*;

/**
 * This interface describes registering tasks with given transfer semantics
 */
public interface TaskRegistrar {
    /**
     * Registering of sourceEndpoint replaces the default implementation
     * @param sourceEndpoint
     */
    void RegisterSourceEndpoint(TransferLifecycle sourceEndpoint);

    void RegisterTableSnapshotTask(TableSnapshotTask snapshotTask, SourceTaskFailoverType failoverType);
    void RegisterStreamTask(StreamTask streamTask, SourceTaskFailoverType failoverType);
}
