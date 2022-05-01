package yandexcloud.datatransfer.dtextension.cdc;

public class Table {
    private final Database database;
    private final String name;

    public Table(String database, String name) {
        this.database = new Database(database);
        this.name = name;
    }

    public String getDatabase() {
        return database.getName();
    }

    public String getName() {
        return name;
    }
}
