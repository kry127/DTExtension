package yandexcloud.datatransfer.dtextension.sink;

import yandexcloud.datatransfer.dtextension.guaranties.SinkTaskWriteSemantics;
import yandexcloud.datatransfer.dtextension.task.*;

/**
 * This interface describes registering tasks with given transfer semantics
 */
public interface TaskRegistrar {
    /**
     * Registering of sourceEndpoint replaces the default implementation
     * @param targetEndpoint
     */
    void RegisterSourceEndpoint(TargetEndpoint targetEndpoint);

    void RegisterSinkTask(SinkTask sinkTask, SinkTaskWriteSemantics sinkSemantics);
}
