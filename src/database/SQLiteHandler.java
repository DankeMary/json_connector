package database;


import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.sqlite.JDBC;

import com.google.common.base.Joiner;

import model.Column;
import model.Table;
import model.TableRow;
import schema.JSONSchema;
import schema.JSONSchemaParser;


public class SQLiteHandler implements DatabaseHandler
{
    private static final String COLUMN_ID = "\"$id\"";
    /*private final String DB_LOCATION = System.getProperty("user.dir")
        + "\\src\\resources\\";*/
    private final String DB_LOCATION = "";
    private String location = DB_LOCATION;
    //private JSONSchema schema;
    private String connString;
    private Connection conn = null;
    //private Map<String, Map<TableRow, Integer>> tablesData = new HashMap<>();
    
    public SQLiteHandler() // throws SQLException ????????
    {
        try
        {
            DriverManager.registerDriver(new JDBC());
            location = DB_LOCATION;
            //schema = null;
            connString = "";
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
        }
    }

    public SQLiteHandler(String dbName)
    {
        //this.schema = schema;
        try
        {
            DriverManager.registerDriver(new JDBC());
            location = DB_LOCATION;
            connString = "jdbc:sqlite:" + location + dbName + ".db";
            conn = DriverManager.getConnection(connString);
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
        }
    }    

    @Override
    public void createTable(Table table, List<Column> columns)
    {
        try
        {            
            conn = DriverManager.getConnection(connString);
            String query = createTableQuery(table, columns);
            PreparedStatement pstmt = conn
                    .prepareStatement(query);                
            pstmt.executeUpdate();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
        }
    }   
    
    private String createTableQuery(Table t, List<Column> cols)
    {
        StringBuilder query = new StringBuilder("CREATE TABLE \"");
        query = query.append(t.getName() + "\" (");
        query.append(COLUMN_ID + " INTEGER PRIMARY KEY ");
        for (Column c : cols)
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
                        + c.getRefTable().getName() + "\"(" + COLUMN_ID + ") ");
                    break;
                case "boolean":
                    query.append(" NUMERIC ");
                    break;
                // default?
            }
        }
        query.append(");");
        //System.out.println(query);
        return query.toString();
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
        if (!checkColumns(t, columns))
            System.out
                .println("1 or more columns don't belong to the given table!");
        else
            try
            {
                //StringBuilder header = new StringBuilder();
                
                StringBuilder query = new StringBuilder("SELECT ");
                for (Column c : columns)
                {
                    //header.append(c.getName() + "    ");
                    query.append("\"" + c.getName() + "\",");
                }
                //header.setLength(header.length() - 1);
                query.setLength(query.length() - 1);
                query.append(" FROM \"" + t.getName() + "\";");

                conn = DriverManager.getConnection(connString);
                PreparedStatement pstmt = conn
                    .prepareStatement(query.toString());
                
                ResultSet res = pstmt.executeQuery();
                List<String> resList = new LinkedList<String>();

                int i = 1; // i = 2
                while (res.next())
                {
                    StringBuilder str = new StringBuilder(/*
                                                           * "\"" +
                                                           * String.valueOf(res.
                                                           * getLong("$id")) +
                                                           * "\""
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
                                boolean valBoolean = res
                                    .getBoolean(c.getName());
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
                System.out.println("getData " + e.getMessage());
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
    public void uploadData(Table table, List<Column> columns, Map<TableRow, Integer> rows)
    {
        try
        {
            conn = DriverManager.getConnection(connString);
            conn.setAutoCommit(false);
            
            String query = createInsertQuery(table, columns);
            
            
            try (PreparedStatement stmt = conn.prepareStatement(query))
            {
                for (TableRow row : rows.keySet())
                {
                    stmt.setInt(1, row.getId());
                    int colIndex = 2;
                    Object[] values = row.getValues();
                    for (Column col : columns)
                    {
                        Object value = values[colIndex - 2];
                        if (value == null)
                        {
                            stmt.setNull(colIndex, Types.VARCHAR);
                        }
                        else
                        {
                            //MIND THE DATATYPE!!!
                            stmt.setObject(colIndex, value);
                        }
                        colIndex++;
                    }
                    stmt.addBatch();
                }

                stmt.executeBatch();
                conn.commit();                
            }
        }
        catch (SQLException e)
        {
            System.out.println("uploadData " + e.getMessage());
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

    private String createInsertQuery(Table table, List<Column> columns)
    {        
        StringBuilder query = new StringBuilder(
            "INSERT INTO \"" + table.getName() + "\"(" + COLUMN_ID + ", ");
        
        Joiner.on(',').appendTo(query,
            columns.stream().map(column -> "\"" + column.getName() + "\"").iterator());
        query.append(") VALUES(?,");
        Joiner.on(',').appendTo(query,
            columns.stream().map(column -> "?").iterator());
        query.append(");");
        /*System.out.println();
        System.out.println(query);
        System.out.println();*/
        return query.toString();
    }
   
    @Override
    public void getData(Table t, List<Column> columns)
    {
        try
        {
            String query = "SELECT * FROM \"" + t.getName() + "\";";
            conn = DriverManager.getConnection(connString);
            PreparedStatement pstmt = conn.prepareStatement(query);
            ResultSet res = pstmt.executeQuery();
            List<String> resList = new LinkedList<String>();

            int i = 2;
            while (res.next())
            {
                StringBuilder str = new StringBuilder(
                    "\"" + String.valueOf(res.getLong("$id")) + "\"");
                for (Column c : columns)
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
            System.out.println("getData " + e.getMessage());
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
