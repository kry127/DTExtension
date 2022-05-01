package yandexcloud.datatransfer.dtextension.task;

/**
 * This is the change data capture of the whole database installation.
 * Intended to be the only one stream task, if used, because other streams are not needed
 */
public interface ClusterStreamTask extends StreamTask {
}
