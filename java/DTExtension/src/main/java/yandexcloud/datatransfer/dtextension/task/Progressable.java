package yandexcloud.datatransfer.dtextension.task;

/**
 * Add an implementation of this interface to this task to override progress calculation
 * for the task. It makes sense to implement this interface only to source task, but also
 * can be implemented by sink task for calculating capacity occupied by worker.
 */
public interface Progressable {
    /**
     * @return the progress that has been made so far by the worker
     */
    float getProgress();
}
