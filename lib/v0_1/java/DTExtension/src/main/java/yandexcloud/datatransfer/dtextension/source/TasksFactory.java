package yandexcloud.datatransfer.dtextension.source;

import yandexcloud.datatransfer.dtextension.task.Task;

public interface TasksFactory {
    /**
     * You should provide method that accepts user configuration of endpoint,
     * analyze settings and additional info about data, and register tasks
     * in tasks registrar with available transfer semantics
     * @param parameters specifies user parameters for endpoint and known nature of data
     * @param registrar object that should be used to register tasks
     * @return result of configuration:
     */
    void RegisterTasks(EndpointConfigParameters parameters, TaskRegistrar registrar);
}
