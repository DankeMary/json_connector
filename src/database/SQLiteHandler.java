package database;


import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.sqlite.JDBC;

import model.Column;
import model.Table;
import schema.JSONSchema;
import schema.JSONSchemaParser;


public class SQLiteHandler implements DatabaseHandler
{
    private final String DB_LOCATION = System.getProperty("user.dir")
        + "\\src\\resources\\";
    private String location = DB_LOCATION;
    private JSONSchema schema;
    private String connString;
    private Connection conn = null;

    public SQLiteHandler() throws SQLException // ????????
    {
        DriverManager.registerDriver(new JDBC());
        location = DB_LOCATION;
        schema = null;
        connString = "";
    }

    public SQLiteHandler(JSONSchema schema)
    {
        this.schema = schema;
        try
        {
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

    @Override
    public void createDatabase(JSONSchema schema)
    {
        try
        {
            connString = "jdbc:sqlite:" + location + schema.getName() + ".db";
            conn = DriverManager.getConnection(connString);
            for (Table t : schema.getTables())
            {
                String query = createTable(t,
                    schema.getTableColumns().get(t.getName()));
                Statement stmt = conn.createStatement();
                stmt.execute(query);
                stmt.close();
            }
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
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
    }

    @Override
    public String createTable(Table t, List<Column> cols)
    {
        StringBuilder query = new StringBuilder("CREATE TABLE \"");
        query = query.append(t.getName() + "\" (");
        query.append("\"$id\" INTEGER PRIMARY KEY ");
        for (Column c : schema.getTableColumns().get(t.getName()))
        {
            query.append(", \"" + c.getName() + "\" ");
            switch (c.getType())
            {
                case "string":
                case "array":
                    query.append(" TEXT ");
                    break;
                case "integer":
                    query.append(" INTEGER ");
                    break;
                case "number":
                    query.append(" REAL ");
                    break;
                case "object":
                    query.append(" INTEGER REFERENCES \"" + schema.getDefName(c.getName()) + "\"(\"$id\") ");
                    break;
                /*
                 * case "boolean": query.append(" INTEGER "); break;
                 */
            }
        }
        query.append(");");
        System.out.println(query.toString());
        return query.toString();
    }

    @Override
    public void deleteDatabase()
    {
        try
        {
            conn.close();
            File file = new File(location + schema.getName() + ".db");
            System.out.println(file.getAbsolutePath());
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
    }

    @Override
    public void getData(Table t, Column... columns)
    {
        //check that columns are actually from that table
        try
        {
        StringBuilder query = new StringBuilder("SELECT * FROM \"" + t.getName() + "\";");
        for(Column c : columns)
        {
            query.append("\"" + c.getName() + "\",");
        }
        query.setLength(query.length() - 1);
        query.append(" FROM \"" + t.getName() + "\";");
        
        conn = DriverManager.getConnection(connString);
        PreparedStatement pstmt = conn.prepareStatement(query.toString());        
        //pstmt.setString(1, t.getName());
        ResultSet res = pstmt.executeQuery();
        List<String> resList = new LinkedList<String>();
        
        int i = 1;
        while (res.next())
        {
            StringBuilder str = new StringBuilder(/*"\"" + String.valueOf(res.getLong("$id")) + "\""*/);
            for(Column c : columns)
            {
                str.append(c.getName() + ": ");
                switch(c.getType())
                {
                    case "string":
                    case "array":
                        String valStr = res.getString(c.getName());                        
                        if (res.wasNull()) 
                            str.append("\"null\"  ");
                        else
                            str.append("\"" + valStr + "\"  ");
                        //System.out.println(valStr);
                        break;
                    case "integer":
                    case "object":
                        long valLong = res.getLong(c.getName());
                        if (res.wasNull()) 
                            str.append("\"null\"  ");
                        else
                            str.append("\"" + valLong + "\"  ");
                        //System.out.println(valLong);
                        break;
                    case "number":
                        double valDouble = res.getDouble(c.getName());
                        if (res.wasNull()) 
                            str.append("\"null\"  ");
                        else
                            str.append("\"" + valDouble + "\"  ");
                        //System.out.println(valDouble);
                        break;
                    /*case "boolean":
                     * break;*/                        
                }
            }
            resList.add(str.toString());
        }
        for(String s : resList)
            System.out.println(s);
        
        } catch (SQLException e)
        {
            System.out.println(e.getMessage());
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
    }

    private void insertData(String query)
    {

    }

    @Override
    public void loadData(JSONObject data)
    {
        try
        {
            conn = DriverManager.getConnection(connString);
            walkThroughAndLoad(schema.getName(), (JSONObject) schema.getSchema().get("properties"), data);
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
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
    }

    private String createInsertQuery(String currTableName)
    {
        List<Column> cols = schema.getTableColumns().get(currTableName);
        StringBuilder query = new StringBuilder(
            "INSERT INTO \"" + currTableName + "\"(");
        for (Column c : cols)
        {
            query.append("\"" + c.getName() + "\",");
        }
        query.setLength(query.length() - 1);
        query.append(") VALUES(");
        for (int i = 0; i < cols.size(); i++)
            query.append("?,");
        query.setLength(query.length() - 1);
        query.append(");");
        System.out.println(query.toString());
        return query.toString();
    }

    public void walkThroughAndLoad(String currTableName, JSONObject currTable, JSONObject data)
    {
        try
        {
            String query = createInsertQuery(currTableName);
            List<Column> cols = schema.getTableColumns().get(currTableName);
            int cnt = 1;
            PreparedStatement pstmt = conn.prepareStatement(query);
            Object value = null;
            for (Column c : cols)
            {
                /*if (c.getType().equals("object") || c.getType().equals("array"))
                    value = data.get(schema.getJSONName(c.getName()));
                else*/
                    value = data.get(c.getName());
                switch (c.getType())
                {
                    //WHAT CAN BE NULL???
                    case "string":                        
                        if (value == null)
                            pstmt.setNull(cnt, java.sql.Types.VARCHAR); //or NVARCHAR?
                        else
                            pstmt.setString(cnt, (String)value);
                        break;
                    case "integer":
                        if (value == null)
                            pstmt.setNull(cnt, java.sql.Types.BIGINT); 
                        else
                            pstmt.setLong(cnt, (long)value);
                        break;
                    case "number":
                        if (value == null)
                            pstmt.setNull(cnt, java.sql.Types.FLOAT); //or DOUBLE PRECISION?
                        else
                            pstmt.setDouble(cnt, (double) data.get(c.getName()));
                        break;
                    case "array":
                        if (value == null)
                            pstmt.setNull(cnt, java.sql.Types.FLOAT); //or DOUBLE PRECISION?
                        else
                        {
                            JSONArray arr = (JSONArray) value;
                            if (arr.size() == 0)
                                pstmt.setString(cnt, ""); //"" OR null?
                            else
                            {
                                StringBuilder res = new StringBuilder();
                                StringBuilder realName = new StringBuilder(c.getName());
                                //schema for field which keeps array
                                JSONObject currSchema = (JSONObject)currTable.get(c.getName());
                                String type = null;
                                if (currSchema.containsKey("$ref"))
                                {
                                    String path = (String) currSchema.get("$ref");
                                    currSchema = JSONSchemaParser.findDef(realName, path,(JSONObject)schema.getSchema());
                                    type = (String)currSchema.get("type");
                                    currSchema = (JSONObject)currSchema.get("items");
                                }
                                else if (((JSONObject)currSchema.get("items")).containsKey("$ref"))
                                {
                                    String path = (String)((JSONObject)currSchema.get("items")).get("$ref");
                                    currSchema = JSONSchemaParser.findDef(realName, path,(JSONObject)schema.getSchema());
                                    type = (String)currSchema.get("type");
                                }
                                /*else if (currSchema.get("type").equals("object"))
                                {
                                    //does it ever go there?
                                    type = c.getName();
                                }
                                else if (((JSONObject) currSchema.get("items")).get("type").equals("object"))
                                    type = c.getName();*/
                                else
                                    {
                                        currSchema = (JSONObject)currSchema.get("items");
                                        type = c.getType();
                                    }
                                
                                if (!type.equals("object"))
                                {
                                    for (int i = 0; i < arr.size(); i++)
                                    {
                                        res.append(arr.get(i) + " ");
                                    }
                                    pstmt.setString(cnt, res.toString());
                                }
                                else
                                {
                                    for (int i = 0; i < arr.size(); i++)
                                    {
                                        JSONObject arrValue = (JSONObject) arr.get(i);
                                        if (arrValue == null || arrValue.size() == 0)
                                            res.append(" null ");  // or space?
                                        else 
                                        {
                                            //currTable - schema, arrValue - value from array
                                            System.out.println(type);
                                            walkThroughAndLoad(realName.toString(), currSchema, arrValue);
                                            Statement st1 = conn.createStatement();
                                            ResultSet rs1 = st1.executeQuery(
                                                "SELECT MAX(\"$id\") AS LAST FROM \"" + realName.toString()
                                                    + "\";");
                                            if (rs1.next())
                                            {
                                                res.append(rs1.getInt("LAST") + " ");
                                                pstmt.setInt(cnt, rs1.getInt("LAST"));
                                            }
                                        }
                                    }
                                    res.setLength(res.length() - 1);
                                    pstmt.setString(cnt, res.toString());
                                }
                            }
                        }                        
                        break;
                    case "object":
                        if (value == null)
                            pstmt.setNull(cnt, java.sql.Types.BIGINT);
                        else
                        {
                            JSONObject currSchema = (JSONObject)currTable.get(c.getName());
                            StringBuilder realName = new StringBuilder();
                            if (currSchema.containsKey("$ref"))
                                currSchema = JSONSchemaParser.findDef(realName,(String) currSchema.get("$ref"), schema.getSchema());
                                    /*((JSONObject)currTable.get(schema.getJSONName(c.getName()))).containsKey("$ref")
                                    ? JSONSchemaParser.findDef(null,(String) ((JSONObject) currTable.get(c.getName())).get("$ref"),
                                        schema.getSchema())
                                    : (JSONObject) currTable.get(c.getName());*/
                            
                            walkThroughAndLoad(realName.toString(),
                                (JSONObject) currSchema.get("properties"),
                                (JSONObject) value);
                            Statement st2 = conn.createStatement();
                            String q = "SELECT MAX(\"$id\") AS LAST FROM \""
                                    + /*c.getName()*/realName + "\";";
                            System.out.println(q);
                            ResultSet rs2 = st2
                                .executeQuery(q);
                            if (rs2.next())
                                pstmt.setInt(cnt, rs2.getInt("LAST"));
                        }
                        break;
                    /*
                     * case "boolean": query.append(" INTEGER "); break;
                     */
                }
                cnt++;
            }

            pstmt.execute();
            System.out.println();
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage() + "        " + currTableName);
        }
        finally
        {
            // deleteDatabase();
            /*
             * try { if (conn != null) { conn.close(); } } catch (SQLException
             * e) { System.out.println(e.getMessage()); }
             */
        }
    }

    @Override
    public void getData(Table t)
    {
        try
        {
        String query = "SELECT * FROM \"" + t.getName() + "\";";
        conn = DriverManager.getConnection(connString);
        PreparedStatement pstmt = conn.prepareStatement(query);        
        //pstmt.setString(1, t.getName());
        ResultSet res = pstmt.executeQuery();
        List<String> resList = new LinkedList<String>();
        
        int i = 2;
        while (res.next())
        {
            StringBuilder str = new StringBuilder("\"" + String.valueOf(res.getLong("$id")) + "\"");
            for(Column c : schema.getTableColumns().get(t.getName()))
            {
                switch(c.getType())
                {
                    case "string":
                    case "array":
                        String valStr = res.getString(c.getName());
                        if (res.wasNull()) 
                            str.append("  \"null\"");
                        else
                            str.append("  \"" + valStr + "\"");
                        System.out.println(valStr);
                        break;
                    case "integer":
                    case "object":
                        long valLong = res.getLong(c.getName());
                        if (res.wasNull()) 
                            str.append("  \"null\"");
                        else
                            str.append("  \"" + valLong + "\"");
                        System.out.println(valLong);
                        break;
                    case "number":
                        double valDouble = res.getDouble(c.getName());
                        if (res.wasNull()) 
                            str.append("  \"null\"");
                        else
                            str.append("  \"" + valDouble + "\"");
                        System.out.println(valDouble);
                        break;
                    /*case "boolean":
                     * break;*/                        
                }
            }
            resList.add(str.toString());
        }
        for(String s : resList)
            System.out.println(s);
        
        } catch (SQLException e)
        {
            System.out.println(e.getMessage());
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
    }
}
