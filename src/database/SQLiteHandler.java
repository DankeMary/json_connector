package database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.json.simple.JSONObject;
import org.sqlite.JDBC;

import model.Column;
import model.Table;
import schema.JSONSchema;

public class SQLiteHandler implements DatabaseHandler
{
    private final String DB_LOCATION = System.getProperty("user.dir") + "\\src\\resources\\";
    private String location = DB_LOCATION;  
    private JSONSchema schema;
    private String connString;

    public SQLiteHandler() throws SQLException //????????
    {
        DriverManager.registerDriver(new JDBC());
        location = DB_LOCATION;   
        schema = null;
        connString = "";
    }
    
    public SQLiteHandler(JSONSchema schema) /*throws SQLException*/ //????????
    {
        try {
            DriverManager.registerDriver(new JDBC()); 
            location = DB_LOCATION;   
            this.schema = schema;
            connString = "jdbc:sqlite:" + location + schema.getName() + ".db";
            createDatabase(this.schema);
        }
        catch (SQLException e)
        {
            System.out.println(e.getStackTrace());
        }
    }
    
    
    private void filterTables(JSONSchema schema)
    {
        
    }
    
    @Override
    public void createDatabase(JSONSchema schema)
    {
        Connection conn = null;
        try
        {
            System.out.println(location);
            connString = "jdbc:sqlite:" + location + schema.getName() + ".db";
            conn = DriverManager.getConnection(connString);
            for(Table t : schema.getTables())
            {
                System.out.println(t.getName());
                String query = createTable(t, schema.getTableColumns().get(t.getName()));                 
                Statement stmt = conn.createStatement();
                stmt.execute(query);                
            }        
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    @Override
    public String createTable(Table t, List<Column> cols)
    {
        StringBuilder query = new StringBuilder("CREATE TABLE ");
        query = query.append(t.getName() + " (");
        query.append("\"$id\" INTEGER PRIMARY KEY ");
        for(Column c : schema.getTableColumns().get(t.getName()))
        {
            query.append(", \"" + c.getName() + "\" ");
            switch(c.getType())
            {
                case "string": 
                case "array":
                    query.append(" TEXT");
                    break;
                case "integer":
                    query.append(" INTEGER ");
                    break;
                case "number":
                    query.append(" REAL ");
                    break;
                case "object":
                    query.append(" INTEGER REFERENCES " + c.getName() + " ");
                    break;
                /*case "boolean":
                 * query.append(" INTEGER ");
                    break;*/
            }            
        }
        query.append(");");
        return query.toString();
    }

    @Override
    public void deleteTable(Table t)
    {
        
    }

    @Override
    public void deleteDatabase()
    {
        
    }

    @Override
    public void getData(Table t, Column... columns)
    {
        
    }

    @Override
    public void loadData(JSONObject data)
    {
        
    }


}
