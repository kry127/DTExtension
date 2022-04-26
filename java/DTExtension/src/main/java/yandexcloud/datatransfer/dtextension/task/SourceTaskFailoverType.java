package yandexcloud.datatransfer.dtextension.task;

/**
 * Enum 'SourceTaskFailoverType' specifies guaranties that developer of connector can produce.
 * 1. If no guaranties can be provided, FULL_RESTART is the only option of restore during the failover.
 * Snapshot tasks will restart over again. Stream tasks continue from where they are (so there may be
 * data losses for Stream data events producers)
 * 2. If source task can persist it's state and reading of data events can be resumed from the same place with
 * the saved state, COMMITABLE is used. Sometimes source database may miss commit point ofter long period
 * of time (e.g. Stream in MySQL binlog or MongoDB oplog), so data losses still can occur
 * 3. If source task that comply to COMMITABLE has guarantee to produce the same sequence of data events after
 * commit point, it can be marked as REPLAYABLE. Using this mode is the fastest way of restoring transfer
 * after failover, but it may cause data losses if guarantee is missing.
 */
public enum SourceTaskFailoverType {
    FULL_RESTART,
    COMMITTABLE,
    REPLAYABLE,
}
