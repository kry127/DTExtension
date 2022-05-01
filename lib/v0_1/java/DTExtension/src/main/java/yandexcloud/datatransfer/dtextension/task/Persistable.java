package yandexcloud.datatransfer.dtextension.task;

import java.io.IOException;

/**
 * Interface 'Persistable' is used to identify objects that can save and recover their state.
 * Format of serialization is defined by user and stored as []byte.
 * <p>
 * If worker defined as COMMITABLE or REPLAYABLE, it should implement this interface.
 * <p>
 * Sometimes Data Transfer may want to persist task state to resume at some point, thus calling `persist` method.
 * This is produce more robust connector which can continue from saved point. If restoration of the state fails,
 * or it is unavailable to some objects. The transfer will retry from beginning or stop completely.
 * <p>
 * The contract of the interface:
 * 1. method `persist` should consistantly save state of the class as array of bytes (maybe except transient objects)
 * 2. method `restore` should restore object state which was saved with `persist` method
 * If serialized `state` variable does not correctly decode to current class, `InvalidClassException` is expected.
 */
public interface Persistable {
    /**
     * Implement persist method to save the state of your worker.
     * Called periodically in order to continue from recent commited point in the case of restarts.
     *
     * @return The state of the worker
     * @throws IOException
     */
    byte[] persist() throws IOException;

    /**
     * This method is called after failover in order to restore task.
     * Basically, the `state` variable comes as return value from dual `persist()` method.
     * The `lastChangeItem` object is last commited ChangeItem known to the sink and may
     * not correspond to the saved state of the worker achieved by `persist` method
     *
     * @param state             the return value of `persist()` method.
     * @param lastChangeItemKey the key of the last change item. If change item specified
     *                          no key, the whole change item represents it's key.
     * @throws IOException
     */
    void restore(byte[] state, byte[] lastChangeItemKey) throws IOException;
}
