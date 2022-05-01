package yandexcloud.datatransfer.dtextension.sink;

/**
 * A target endpoint is an orchestrator of tasks.
 * If you want more precise control of lifecycle of the tasks -- implement it
 * and register in TaskRegistrar.
 */
public interface TargetEndpoint {
    /**
     * Activate is called on transfer activation. That means that here you should provision
     * resources, for example, check that database is reachable, or technical transfer table is writable
     * <p>
     * Since multiple transfers from source may read changes, use 'transferId' to uniquely identify
     * resources which belong to the transfer, e.g. row of the technical table for writing
     */
    void Activate(String transferId);

    /**
     * Deactivate is called on transfer deactivation. That means that here you should destroy
     * resources provisioned by dual Activate method if they are no longer needed
     */
    void Deactivate(String transferId);
}
