package yandexcloud.datatransfer.dtextension.task;

/**
 * SnapshotTask is a type of Source task that performs 'snapshotting': a phase which
 * reads full copy of object in database and streams it further.
 * <p>
 * SnapshotTask can be simple or splittable, the second one is needed in order to
 * increase bandwidth speed on very big tables.
 */
public interface SnapshotTask extends SourceTask {
}
