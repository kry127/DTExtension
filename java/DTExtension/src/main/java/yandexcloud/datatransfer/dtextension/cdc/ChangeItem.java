package yandexcloud.datatransfer.dtextension.cdc;

public abstract class ChangeItem {
    /**
     * Should implement this method in order to get binary transferrable representation of elementary change unit
     *
     * @return serialization of change item
     */
    abstract byte[] getSerialized();

    /**
     * Returns serialized form of primary key (if any).
     * This method is needed for network efficiency when saving and restoring state of the task.
     * By default returns full serialization of change item as a super-key.
     *
     * @return serialization of key of change item
     */
    byte[] getSerializedKey() {
        return this.getSerialized();
    }

    /**
     * Use this factory method to produce change item
     *
     * @param data
     */
    public static ChangeItem make(byte[] data) {
        return new SimpleChangeItem(data);
    }

    /**
     * Use this factory method to produce keyed change item
     *
     * @param key  a key of change item
     * @param data a data of change item
     */
    public static ChangeItem make(byte[] key, byte[] data) {
        return new KeyedChangeItem(key, data);
    }
}
