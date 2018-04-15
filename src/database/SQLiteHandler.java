package database;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
    private Connection conn = null;

    public SQLiteHandler() throws SQLException //????????
    {
        DriverManager.registerDriver(new JDBC());
        location = DB_LOCATION;   
        schema = null;
        connString = "";
    }
    
    public SQLiteHandler(JSONSchema schema) /*throws SQLException*/ //????????
    {
        this.schema = schema;
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
        //Connection conn = null;
        try
        {
            System.out.println(location);
            connString = "jdbc:sqlite:" + location + schema.getName() + ".db";
            conn = DriverManager.getConnection(connString);
            for(Table t : schema.getTables())
            {
                System.out.println(t.getName());
                String query = createTable(t, schema.getTableColumns().get(t.getName()));    
                System.out.println(query);
                Statement stmt = conn.createStatement();
                stmt.execute(query);   
                stmt.close();
            }      
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("createDatabase :   " + e.getMessage());
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Override
    public String createTable(Table t, List<Column> cols)
    {
        StringBuilder query = new StringBuilder("CREATE TABLE \"");
        query = query.append(t.getName() + "\" (");
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
                    query.append(" INTEGER REFERENCES \"" + c.getName() + "\" ");
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
        try{
            //close connection before executing
            File file = new File(location + schema.getName() + ".db");
            System.out.println(file.getAbsolutePath());
            if(file.delete()){
                System.out.println(file.getName() + " is deleted!");
            }else{
                System.out.println("Delete operation is failed.");
            }
           
        } catch(Exception e){            
            System.out.println(e.getMessage());            
        }
    }

    @Override
    public void getData(Table t, Column... columns)
    {
        
    }

    private void insertData(String query)
    {
        //new connection for every record?
       /* Connection conn = null;
        try
        {
            conn = DriverManager.getConnection(connString);
            for(Table t : schema.getTables())
            {
                System.out.println(t.getName());
                //String query = createTable(t, schema.getTableColumns().get(t.getName()));                 
                Statement stmt = conn.createStatement();
                stmt.execute(query);   
                stmt.close();
            }      
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }*/
    }
    @Override
    public void loadData(JSONObject data)
    {
        try
        {
            conn = DriverManager.getConnection(connString);
        
            walkThroughAndLoad(schema.getName(), (JSONObject)schema.getSchema().get("properties"), data);
            conn.close();    
        }catch (SQLException e)
        {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    private String createInsertQuery(String currTableName)
    {
        List<Column> cols = schema.getTableColumns().get(currTableName);
        StringBuilder query = new StringBuilder("INSERT INTO \"" + currTableName + "\"(");
        for(Column c : cols)
        {
            query.append("\"" + c.getName() + "\",");
        }
        //needed?
        if (query.length() > 0) 
        {
            query.setLength(query.length() - 1);
        }
        query.append(") VALUES(");
        for(int i = 0; i < cols.size(); i++)
            query.append("?,");
        if (query.length() > 0) 
        {
            query.setLength(query.length() - 1);
        }
        query.append(");");
        //System.out.println(query.toString());
        return query.toString();
    }
    public void walkThroughAndLoad(String currTableName, JSONObject currTable, JSONObject data)
    {
        //Connection conn = null;
        try
        {
            //conn = DriverManager.getConnection(connString);
            String query = createInsertQuery(currTableName);
            List<Column> cols = schema.getTableColumns().get(currTableName);
            int cnt = 1;
            PreparedStatement pstmt = conn.prepareStatement(query);          
            for(Column c : cols)
            {
                //if(c.getType().equals)
                switch(c.getType())
                {
                    case "string":
                        System.out.println(c.getName() + " : " + (String)data.get(c.getName()));
                        pstmt.setString(cnt, (String)data.get(c.getName()));
                        break;
                    case "integer":
                        System.out.println(c.getName() + " : " + data.get(c.getName()));
                        pstmt.setLong(cnt, (long)data.get(c.getName()));
                        break;
                    case "number":
                        System.out.println(c.getName() + " : " + (double)data.get(c.getName()));
                        pstmt.setDouble(cnt, (double)data.get(c.getName()));
                        break;
                    /*case "array":
                        
                        break;*/
                    case "object":
                        walkThroughAndLoad(c.getName(), (JSONObject)null, (JSONObject)data.get(c.getName()));
                        Statement st = conn.createStatement();
                        ResultSet rs = st.executeQuery("SELECT MAX(\"$id\") AS LAST FROM \"" + c.getName() + "\";");
                        if(rs.next())
                            {System.out.println(c.getName() + " : " + rs.getInt("LAST"));
                            pstmt.setInt(cnt, rs.getInt("LAST"));}
                        else
                            //pstmt.setNull(cnt, sqlType);
                            pstmt.setInt(cnt, -1);
                        break;
                    /*case "boolean":
                     * query.append(" INTEGER ");
                        break;*/
                }     
                cnt++;
            }
            
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println("walkThroughAndLoad :  " + e.getMessage());
        }
        finally {
            //deleteDatabase();
            /*try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }*/
        }
        
        //hashmap column - value?        
    }
    //private void loadData() {}
}
