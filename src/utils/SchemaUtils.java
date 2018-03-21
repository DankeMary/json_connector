package utils;


import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import model.Column;
import model.Table;
import model.User;


public class SchemaUtils
{
    public static final String USER_NAME = "JSON";

    public static String firstUpperCase(String word)
    {
        if (word == null || word.isEmpty())
            return "";
        return word.substring(0, 1).toUpperCase() + word.substring(1);
    }

    public static String getTableName(String path)
    {
        Path p = Paths.get(path);                
        String fname = p.getFileName().toString();
        int pos = fname.lastIndexOf("."); 
        if (pos > 0) 
            fname = fname.substring(0, pos); 
        return fname; 
    }

    public static String formatName(String str)
    {
        if (str.isEmpty())
            return "";
        String[] parts = str.trim().split("[-_. ]");
        StringBuilder finStr = new StringBuilder(parts[0]);
        for (int i = 1; i < parts.length; i++)
            finStr.append(firstUpperCase(parts[i]));
        return finStr.toString();
    }

    public static void parseSchemaProperties(Table parentTable,
        List<Table> tables, List<Column> columns, User user, JSONObject props)
    {
        for (Object keyObj : props.keySet())
        {
            String key = (String) keyObj;
            JSONObject colData = (JSONObject) props.get(key);

            Column newCol = new Column();
            newCol.setName(formatName(key));
            newCol.setType((String) colData.get("type"));
            newCol.setComment((String) colData.get("description"));
            newCol.setTable(parentTable);
            columns.add(newCol);

            if (newCol.getType().equals("object"))
            {
                Table newTable = new Table();
                newTable.setName(formatName(key));
                newTable.setType(Table.TYPE_TABLE);
                newTable.setOwner(user);
                newTable.setComment((String) colData.get("description"));
                tables.add(newTable);
                parseSchemaProperties(newTable, tables, columns, user,
                    (JSONObject) colData.get("properties"));
            }
        }
    }

    public static JSONObject getJson(String path)
    {
        try
        {
            JSONParser parser = new JSONParser();
            return (JSONObject) parser.parse(new FileReader(path));
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return null;
        }
        catch (ParseException e)
        {
            e.printStackTrace();
            return null;
        }
    }

    public static void parseJsonSchema(String path)
    {
        JSONObject schema = getJson(path);
        
        LinkedList<Table> tables = new LinkedList<Table>();
        LinkedList<Column> columns = new LinkedList<Column>();

        User user = new User();
        user.setName(USER_NAME);
        
        Table rootTable = new Table();
        rootTable.setName(getTableName(path));
        rootTable.setType(Table.TYPE_TABLE);
        rootTable.setOwner(user);
        rootTable.setComment((String)schema.get("description"));

        JSONObject properties = (JSONObject)schema.get("properties");

        parseSchemaProperties(rootTable, tables, columns, user, properties);       
    }
}
