package yandexcloud.datatransfer.dtextension.cdc;

/**
 * This class represents a simple single change event.
 * It is just an alias to byte array
 */
public class SimpleChangeItem extends ChangeItem {
    private final byte[] serialized;

    public SimpleChangeItem(byte[] serializedChangeItem) {
        this.serialized = serializedChangeItem;
    }

    public byte[] getSerialized() {
        return serialized;
    }
}
