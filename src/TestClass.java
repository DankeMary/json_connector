import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.json.simple.JSONObject;

import database.SQLiteHandler;
import model.Table;
import model.TableRow;
import schema.JSONSchema;
import schema.JSONSchemaParser;
import utils.SchemaUtils;

public class TestClass
{
    //schemaPath = "e:\\address.json"
    //dataPath = "e:\\try.json"
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
        
        for(Table table: sortedTables)
        {
            dbHandler.createTable(table, schema.getColumns(table));
        }
        
        for (Table table : sortedTables)
        {
            //dbHandler.createTable(table, schema.getColumns(table));
            Map<TableRow, Integer> rows = tablesData.get(table.getName());
            dbHandler.uploadData(table, schema.getColumns(table), rows);            
            
        }
    }
    
  //ЧТО С НИМ ДЕЛАТЬ???
    /*public void createDatabase(JSONSchema schema)
    {
        try
        {
            connString = "jdbc:sqlite:" + location + schema.getName() + ".db";
            conn = DriverManager.getConnection(connString);
            for (Table t : schema.getTables())
            {
                String query = createTable(t,
                    schema.getColumns(t));
                Statement stmt = conn.createStatement();
                stmt.execute(query);
                stmt.close();
            }
            conn.close(); // is needed here? finally
        }
        catch (SQLException e)
        {
            System.out.println("createDatabase " + e.getMessage());
        }
        finally
        {
            try
            {
                if (conn != null)
                {
                    conn.close();
                }
            }
            catch (SQLException e)
            {
                System.out.println(e.getMessage());
            }
        }
    }*/
    
    /*public void deleteDatabase()
    {
        try
        {
            conn.close();
            File file = new File(location + schema.getName() + ".db");
            if (file.delete())
            {
                System.out.println(file.getName() + " is deleted!");
            }
            else
            {
                System.out.println("Delete operation is failed.");
            }
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
    }*/
}
