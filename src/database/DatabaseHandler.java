package database;

import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

import model.Column;
import model.Table;
import model.TableRow;
import schema.JSONSchema;

public interface DatabaseHandler
{
    //void createDatabase(JSONSchema schema);
    void createTable(Table t, List<Column> cols);
    //void deleteDatabase();
    void getData(Table t, Column... columns);
    void getData(Table t, List<Column> columns);
    void uploadData(Table table, List<Column> columns, Map<TableRow, Integer> rows);
}
