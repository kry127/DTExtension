package yandexcloud.datatransfer.dtextension.guaranties;

/**
 * Enum `SinkTaskWriteSemantics` is indicator of write mode to destination.
 * Usually, you shouldn't bother and use AT_LEAST_ONCE, e.g. in case of
 * restart and restore of the writing worker, we'll write repeated uncommited
 * data events twice.
 * <p>
 * If there is such possibility as distributed transactions in destination and
 * user has asked about exactly once semantics, you may implement EXACTLY_ONCE
 * semantics and mark worker with EXACTLY_ONCE marker, although nothing changes
 * in behaviour of Data Transfer.
 */
public enum SinkTaskWriteSemantics {
    AT_LEAST_ONCE,
    EXACTLY_ONCE
}
