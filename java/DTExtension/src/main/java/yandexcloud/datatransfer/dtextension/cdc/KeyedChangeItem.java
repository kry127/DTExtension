package yandexcloud.datatransfer.dtextension.cdc;

/**
 * This class represents a single change event where user specifies the key of change event
 */
public class KeyedChangeItem extends ChangeItem {
    private final byte[] serializedKey;
    private final byte[] serializedItem;

    public KeyedChangeItem(byte[] serializedKey, byte[] serializedChangeItem) {
        this.serializedKey = serializedChangeItem;
        this.serializedItem = serializedChangeItem;
    }

    public byte[] getSerialized() {
        return serializedItem;
    }

    @Override
    public byte[] getSerializedKey() {
        return serializedKey;
    }
}
