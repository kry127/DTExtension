package yandexcloud.datatransfer.dtextension.sink;

public interface TasksFactory {
    /**
     * You should provide method that accepts user configuration of endpoint,
     * analyze settings and additional info about data, and register tasks
     * in tasks registrar with available transfer semantics
     * @param parameters specifies user parameters for endpoint and known nature of data
     * @param registrar object that should be used to register tasks
     */
    void RegisterTasks(EndpointConfigParameters parameters, TaskRegistrar registrar);
}
