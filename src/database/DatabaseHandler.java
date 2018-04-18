package database;

import java.util.List;

import org.json.simple.JSONObject;

import model.Column;
import model.Table;
import schema.JSONSchema;

public interface DatabaseHandler
{
    void createDatabase(JSONSchema schema);
    String createTable(Table t, List<Column> cols);
    void deleteDatabase();
    void getData(Table t, Column... columns);
    void getData(Table t);
    void loadData(JSONObject data);
}
