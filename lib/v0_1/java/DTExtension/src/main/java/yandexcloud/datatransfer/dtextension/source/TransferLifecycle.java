package yandexcloud.datatransfer.dtextension.source;

/**
 * A source endpoint is an orchestrator of tasks.
 * If you want more precise control of lifecycle of the tasks -- implement it
 * and register in TaskRegistrar.
 *
 * The default activation behaviour: first is activation of StreamTasks, second is activation of SnapshotTasks.
 * This guaranties AtLeastOnce semantics. If you need ExactlyOnceSemantics you may implement more intricate
 * way of activation, for instance, transactional saving of LSN for table Snapshot and Stream.
 */
public interface TransferLifecycle {
    /**
     * Activate is called on transfer activation. That means that here you should provision
     * resources: create slot for PostgreSQL, or maybe create table for healtheck pings,
     * or take a snapshot lock, et cetera.
     * <p>
     * Since multiple transfers from source may read changes, use 'transferId' to uniquely identify
     * resources which belong to the transfer, e.g. slot name in PostgreSQL
     */
    void Activate(String transferId);

    /**
     * Deactivate is called on transfer deactivation. That means that here you should destroy
     * resources provisioned by dual Activate method: for example slot for PostgreSQL
     * <p>
     * Since multiple transfers from source may read changes, use 'transferId' to uniquely identify
     * resources which belong to the transfer, e.g. slot name in PostgreSQL
     */
    void Deactivate(String transferId);
}
