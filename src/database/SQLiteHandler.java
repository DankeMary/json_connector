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

    public SQLiteHandler() // throws SQLException  ????????
    {
        try
        {
        DriverManager.registerDriver(new JDBC());
        location = DB_LOCATION;
        schema = null;
        connString = "";
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
        }
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
            System.out.println(e.getMessage());
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
                String query = createTable(t, schema.getTableColumns().get(t.getName()));
                Statement stmt = conn.createStatement();
                stmt.execute(query);
                stmt.close();
            }
            conn.close(); //нцжно ли здесь?? finally
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
                    query.append(" INTEGER REFERENCES \""
                        + schema.getDefName(c.getName()) + "\"(\"$id\") ");
                    break;
                case "boolean": 
                    query.append(" NUMERIC "); 
                    break;
                //default?
            }
        }
        query.append(");");
        //System.out.println(query.toString());
        return query.toString();
    }

    @Override
    public void deleteDatabase()
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
    }

    private boolean checkColumns(Table t, Column... columns)
    {
        for (Column c : columns)
        {
            if (!t.getName().equals(c.getTable().getName()))
                return false;
        }
        return true;
    }
    @Override
    public void getData(Table t, Column... columns)
    {
        // check that columns are actually from that table
        if (!checkColumns(t, columns))
            System.out.println("1 or some columns don't belong to the given table!");
        else
            try
            {
                StringBuilder query = new StringBuilder(
                    "SELECT * FROM \"" + t.getName() + "\";");
                for (Column c : columns)
                {
                    query.append("\"" + c.getName() + "\",");
                }
                query.setLength(query.length() - 1);
                query.append(" FROM \"" + t.getName() + "\";");
    
                conn = DriverManager.getConnection(connString);
                PreparedStatement pstmt = conn.prepareStatement(query.toString());
                // pstmt.setString(1, t.getName());
                ResultSet res = pstmt.executeQuery();
                List<String> resList = new LinkedList<String>();
    
                int i = 1;  //i = 2
                while (res.next())
                {
                    StringBuilder str = new StringBuilder(/*
                                                           * "\"" +
                                                           * String.valueOf(res.
                                                           * getLong("$id")) + "\""
                                                           */);
                    for (Column c : columns)
                    {
                        str.append(c.getName() + ": ");
                        switch (c.getType())
                        {
                            case "string":
                            case "array":
                                String valStr = res.getString(c.getName());
                                if (res.wasNull())
                                    str.append("\"null\",  ");
                                else
                                    str.append("\"" + valStr + "\",  ");
                                break;
                            case "integer":
                            case "object":
                                long valLong = res.getLong(c.getName());
                                if (res.wasNull())
                                    str.append("\"null\" , ");
                                else
                                    str.append("\"" + valLong + "\",  ");
                                break;
                            case "number":
                                double valDouble = res.getDouble(c.getName());
                                if (res.wasNull())
                                    str.append("\"null\",  ");
                                else
                                    str.append("\"" + valDouble + "\",  ");
                                break;
                            
                            case "boolean": 
                                boolean valBoolean = res.getBoolean(c.getName());
                                if (res.wasNull())
                                    str.append("\"null\",  ");
                                else
                                    str.append("\"" + valBoolean + "\",  ");
                                break;
                             
                        }
                    }
                    str.setLength(str.lastIndexOf(","));
                    resList.add(str.toString());
                }
                printData(resList);
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

    public void printData(List<String> resList)
    {
        for (String s : resList)
            System.out.println(s);
    }

    @Override
    public void uploadData(JSONObject data)
    {
        try
        {
            conn = DriverManager.getConnection(connString);
            walkAndLoad(schema.getName(),
                (JSONObject) schema.getSchema().get("properties"), data);
            // mind : finally
            // conn.close();
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
        return query.toString();
    }

    private void walkAndLoad(String currTableName, JSONObject currTable,
        JSONObject data)
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
                /*
                 * if (c.getType().equals("object") ||
                 * c.getType().equals("array")) value =
                 * data.get(schema.getJSONName(c.getName())); else
                 */
                value = data.get(c.getName());
                switch (c.getType())
                {
                    case "string":
                        if (value == null)
                            pstmt.setNull(cnt, java.sql.Types.VARCHAR); 
                        else
                            pstmt.setString(cnt, (String) value);
                        break;
                    case "integer":
                        if (value == null)
                            pstmt.setNull(cnt, java.sql.Types.BIGINT);
                        else
                            pstmt.setLong(cnt, (long) value);
                        break;
                    case "number":
                        if (value == null)
                            pstmt.setNull(cnt, java.sql.Types.FLOAT); // or
                                                                      // DOUBLE
                                                                      // PRECISION?
                        else
                            pstmt.setDouble(cnt,
                                (double) data.get(c.getName()));
                        break;
                    case "array":
                        if (value == null)
                            pstmt.setNull(cnt, java.sql.Types.VARCHAR); 
                        else
                        {
                            JSONArray arr = (JSONArray) value;
                            if (arr.size() == 0)
                                pstmt.setString(cnt, ""); // "" OR null?
                            else
                            {
                                StringBuilder res = new StringBuilder();
                                StringBuilder realName = new StringBuilder(
                                    c.getName());
                                // schema for field which keeps array
                                JSONObject currSchema = (JSONObject) currTable
                                    .get(c.getName());
                                String type = null;
                                if (currSchema.containsKey("$ref"))
                                {
                                    String path = (String) currSchema
                                        .get("$ref");
                                    currSchema = JSONSchemaParser.findDef(
                                        realName, path,
                                        (JSONObject) schema.getSchema());
                                    currSchema = (JSONObject) currSchema
                                        .get("items");
                                    type = (String) currSchema.get("type");
                                    
                                }
                                else if (((JSONObject) currSchema.get("items"))
                                    .containsKey("$ref"))
                                {
                                    String path = (String) ((JSONObject) currSchema
                                        .get("items")).get("$ref");
                                    currSchema = JSONSchemaParser.findDef(
                                        realName, path,
                                        (JSONObject) schema.getSchema());
                                    type = (String) currSchema.get("type");
                                }
                                else
                                {
                                    currSchema = (JSONObject) currSchema.get("items");
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
                                        JSONObject arrValue = (JSONObject) arr
                                            .get(i);
                                        if (arrValue == null
                                            || arrValue.size() == 0)
                                            res.append(" null "); // or space?
                                        else
                                        {
                                            // currTable - schema, arrValue -
                                            // value from array
                                            System.out.println(type);
                                            walkAndLoad(realName.toString(), (JSONObject)currSchema.get("properties"), arrValue);
                                            Statement st1 = conn
                                                .createStatement();
                                            ResultSet rs1 = st1.executeQuery(
                                                "SELECT MAX(\"$id\") AS LAST FROM \""
                                                    + realName.toString()
                                                    + "\";");
                                            if (rs1.next())
                                            {
                                                int val = rs1.getInt("LAST");
                                                res.append(val + " ");
                                                pstmt.setInt(cnt, val);
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
                            JSONObject currSchema = (JSONObject) currTable
                                .get(c.getName());
                            StringBuilder realName = new StringBuilder();
                            if (currSchema.containsKey("$ref"))
                                currSchema = JSONSchemaParser.findDef(realName,
                                    (String) currSchema.get("$ref"),
                                    schema.getSchema());
                            /*
                             * ((JSONObject)currTable.get(schema.getJSONName(c.
                             * getName()))).containsKey("$ref") ?
                             * JSONSchemaParser.findDef(null,(String)
                             * ((JSONObject)
                             * currTable.get(c.getName())).get("$ref"),
                             * schema.getSchema()) : (JSONObject)
                             * currTable.get(c.getName());
                             */

                            walkAndLoad(realName.toString(),
                                (JSONObject) currSchema.get("properties"),
                                (JSONObject) value);
                            Statement st2 = conn.createStatement();
                            String q = "SELECT MAX(\"$id\") AS LAST FROM \""
                                + realName + "\";";
                            ResultSet rs2 = st2.executeQuery(q);
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
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
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
            // pstmt.setString(1, t.getName());
            ResultSet res = pstmt.executeQuery();
            List<String> resList = new LinkedList<String>();

            int i = 2;
            while (res.next())
            {
                StringBuilder str = new StringBuilder(
                    "\"" + String.valueOf(res.getLong("$id")) + "\"");
                for (Column c : schema.getTableColumns().get(t.getName()))
                {
                    switch (c.getType())
                    {
                        case "string":
                        case "array":
                            String valStr = res.getString(c.getName());
                            if (res.wasNull())
                                str.append("  \"null\"");
                            else
                                str.append("  \"" + valStr + "\"");
                            // System.out.println(valStr);
                            break;
                        case "integer":
                        case "object":
                            long valLong = res.getLong(c.getName());
                            if (res.wasNull())
                                str.append("  \"null\"");
                            else
                                str.append("  \"" + valLong + "\"");
                            // System.out.println(valLong);
                            break;
                        case "number":
                            double valDouble = res.getDouble(c.getName());
                            if (res.wasNull())
                                str.append("  \"null\"");
                            else
                                str.append("  \"" + valDouble + "\"");
                            // System.out.println(valDouble);
                            break;
                        /*
                         * case "boolean": break;
                         */
                    }
                }
                resList.add(str.toString());
            }
            for (String s : resList)
                System.out.println(s);

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
}
