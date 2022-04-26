package yandexcloud.datatransfer.dtextension.task;

/**
 * This task is like TableSnapshotTask, but able to split it's work.
 * Use case scenario: when big table is copying in one thead by the sink, it may want
 * to split heavy task on two parts. Thus, heavy tables can be split when uploading
 * in order to increase bandwidth
 */
public interface SplittableTableSnapshotTask extends TableSnapshotTask {
    /**
     * This method should split current task by at least two subtask.
     * Warning: after invoking this method, no further change items will be extracted
     * from this parent task. For instance, let's assume you are copying
     * table rows in range [#min, #max] and stopped at the line #l. So, you can
     * split task by two parts: [#l, #m), [#m, #max], where #m is between #l and #max.
     * Thus, sink could read changes in parallel.
     * <p>
     * A good usage of this class is persistable, independently working tasks with
     * no shared resources used by siblings. But if this task is ephemereal with no
     * guaranties, you generally have no restriction on implementation of subtasks.
     *
     * @return Subtasks of thi tasks
     */
    SplittableTableSnapshotTask[] split();
}
