package utility;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.json.simple.JSONObject;

import database.SQLiteHandler;
import handler.DataHandler;
import model.Column;
import model.Table;
import model.TableRow;
import schema.JSONSchema;
import schema.JSONSchemaParser;
import utils.SchemaUtils;

public class Utility
{
    private String dbName;
    private JSONSchema schema;
    private JSONObject data;
    private JSONSchemaParser schemaParser;
    private DataHandler dataHandler;    
    private Map<String, Map<TableRow, Integer>> tablesData;
    private List<Table> sortedTables;
    private SQLiteHandler dbHandler;
    
    public Utility()
    {
        dbName = null;
        schema = null;
        data = null;
        schemaParser = null;
        dataHandler = null;    
        tablesData = null;
        sortedTables = null;
        dbHandler = null;
    }
    
    /**
     * Загрузка JSON-схемы из файла
     * @param schemaPath
     */
    public void uploadSchema(String schemaPath)
    {
        dbName = SchemaUtils.getTableName(schemaPath);
        schema = new JSONSchema(dbName,
            SchemaUtils.getJson(schemaPath));
        schemaParser = new JSONSchemaParser(schema);
    }
    
    /**
     * Загрузка данных в формате JSON из файла
     * @param dataPath
     */
    public void uploadData(String dataPath)
    {
        data = SchemaUtils.getJson(dataPath);        

        dataHandler = new DataHandler(schema, data);
        dataHandler.handleData();
        
        tablesData = dataHandler.getTablesData();

        sortedTables = schema.getTables().stream()
            .sorted((table1, table2) -> schemaParser.getLevel(table1)
                - schemaParser.getLevel(table2))
            .collect(Collectors.toList());
    }
    
    /**
     * Создание базы данных на основе JSON-схемы
     */
    public void createDatabase()
    {
        dbHandler = new SQLiteHandler(dbName);

        for (Table table : sortedTables)
        {
            dbHandler.createTable(table, schema.getColumns(table));
        }
    }
    /**
     * Загрузка входных данных в базу данных
     */
    public void fillDatabase()
    {
        for (Table table : sortedTables)
        {
            Map<TableRow, Integer> rows = tablesData.get(table.getName());
            dbHandler.uploadData(schema.getTableColumns(), table, rows);
        }
    }

    /**
     * Выборка из базы данных по указанным столбцам таблицы
     * @param tableName
     * @param columnsNames
     * @return
     */
    public List<String> getColumnsData(String tableName, List<String> columnsNames)
    {
        Table table = schema.getTable(tableName);
        
        List<Column> columns = schema.getColumns(tableName, columnsNames);
        
        return dbHandler.getData(table, columns);
    }
    
    /**
     * Выборка из базы данных по всем столбцам таблицы
     * @param tableName
     * @return
     */
    public List<String> getAllData(String tableName)
    {
        Table table = schema.getTable(tableName);
        
        List<Column> columns = schema.getColumns(table);       
        
        return dbHandler.getAllData(table, columns);
    }     
    
    /**
     * Печать названий всех таблиц и их столбцов
     */
    public void printTablesColumns()
    {
        for (Map.Entry<String, List<Column>> entry : schema.getTableColumns().entrySet()) {
             System.out.println("Таблица: " + entry.getKey());
             List<Column> cols = entry.getValue();
             for(Column c : cols)
                 System.out.println("    " + c.getName());
         } 
    }
}
