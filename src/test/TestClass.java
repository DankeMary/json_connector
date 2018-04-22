package test;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.json.simple.JSONObject;

import database.SQLiteHandler;
import handlers.DataHandler;
import model.Table;
import model.TableRow;
import schema.JSONSchema;
import schema.JSONSchemaParser;
import utils.SchemaUtils;

public class TestClass
{
    public static void runTest(String schemaPath, String dataPath)
    { 
        String dbName = SchemaUtils.getTableName(schemaPath);
        JSONSchema schema =  new JSONSchema(dbName, SchemaUtils.getJson(schemaPath));
        JSONObject data = SchemaUtils.getJson(dataPath);
        JSONSchemaParser schemaParser = new JSONSchemaParser(schema);
        
        DataHandler dataHandler = new DataHandler(schema, data);
        dataHandler.handleData();
        Map<String, Map<TableRow, Integer>> tablesData = dataHandler.getTablesData();
        
        List<Table> sortedTables = schema.getTables().stream()
                .sorted((table1, table2) -> schemaParser.getLevel(table1) - schemaParser.getLevel(table2))
                .collect(Collectors.toList());
        
        SQLiteHandler dbHandler = new SQLiteHandler(dbName);
        
        for(Table table : sortedTables)
        {
            dbHandler.createTable(table, schema.getColumns(table));
        }
        
        for (Table table : sortedTables)
        {
            Map<TableRow, Integer> rows = tablesData.get(table.getName());
            dbHandler.uploadData(schema.getTableColumns(), table, rows);           
        }
    }    
}
