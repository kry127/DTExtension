package yandexcloud.datatransfer.dtextension.task;

/**
 * Task is a unit of work for endpoint. Task has it's lifecycle and purpose.
 * Depending on the type of the task, Data Transfer can perform certain actions to
 * maintain the state of the transfer, request new data when it is needed.
 * <p>
 * Because Tasks provide lifecycle endpoint hooks you can control the state of the
 * database to perform actions: for example, take necessary locks, getting limits
 * of transferrable data, et cetera.
 */
public interface Task {
    /**
     * This method is called when transfer is activating. Use this method
     * to initialize network resources, create tables, take necessary locks, et cetera.
     * <p>
     * Since multiple transfers from source may read changes, use 'transferId' to uniquely identify
     * resources which belong to the transfer, e.g. slot name in PostgreSQL
     */
    void Activate(String transferId);

    /**
     * This method is called when transfer is deactivating. Use this method
     * to cleanup resources, especially used in databases where you connected
     * <p>
     * Since multiple transfers from source may read changes, use 'transferId' to uniquely identify
     * resources created in Activate phase that should be destroyed, e.g. slot name in PostgreSQL
     */
    void Deactivate(String transferId);
}
