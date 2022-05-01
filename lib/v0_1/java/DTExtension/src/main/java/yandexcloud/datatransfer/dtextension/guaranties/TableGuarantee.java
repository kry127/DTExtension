package yandexcloud.datatransfer.dtextension.guaranties;

import yandexcloud.datatransfer.dtextension.cdc.Table;

public class TableGuarantee {
    private final Table table;
    private final boolean additionOnly;
    private final MonoColumnDescription monoColumnDescription;

    public TableGuarantee(Table table, boolean additionOnly, MonoColumnDescription monoColumnDescription) {
        this.table = table;
        this.additionOnly = additionOnly;
        this.monoColumnDescription = monoColumnDescription;
    }

    public Table getTable() {
        return table;
    }

    public boolean isAdditionOnly() {
        return additionOnly;
    }

    public MonoColumnDescription getMonoColumnDescription() {
        return monoColumnDescription;
    }
}
