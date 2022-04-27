package yandexcloud.datatransfer.dtextension.guaranties;

public class MonoColumnDescription {
    private final String columnName;
    private final boolean strict;
    private final boolean descending;

    public MonoColumnDescription(String columnName, boolean strict, boolean descending) {
        this.columnName = columnName;
        this.strict = strict;
        this.descending = descending;
    }

    public String getColumnName() {
        return columnName;
    }

    public boolean isStrict() {
        return strict;
    }

    public boolean isDescending() {
        return descending;
    }
}
