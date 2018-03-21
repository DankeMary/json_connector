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
        String fileName = p.getFileName().toString();
        String[] parts = fileName.split(".json");
        return parts[0];

        /*
         * String fname = file.getName(); int pos = fname.lastIndexOf("."); if
         * (pos > 0) { fname = fname.substring(0, pos); }
         */
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
                newTable.setType(Table.TABLE_TYPE);
                newTable.setOwner(user);
                newTable.setComment((String) colData.get("description"));
                tables.add(newTable);
                parseJsonSchema(newTable, tables, columns, user,
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

    // append table name?
    public static void parseJsonSchema(String path)
    {
        LinkedList<Table> tables = new LinkedList<Table>();
        LinkedList<Column> columns = new LinkedList<Column>();

        User user = new User();
        user.setName(USER_NAME);

        // String s_path = "e:\\address.json";

        /*
         * try { JSONParser parser = new JSONParser(); JSONObject schema =
         * (JSONObject)parser.parse(new FileReader(s_path));
         */
        JSONObject schema = getJson(path);
        Table t1 = new Table();

        t1.setName(getTableName(s_path));
        t1.setType(Table.TABLE_TYPE);
        t1.setOwner(user);
        t1.setComment((String) schema.get("description"));

        JSONObject properties = (JSONObject) schema.get("properties");

        parseJsonSchema(t1, tables, columns, user, properties);
        /*
         * }catch( IOException e) { e.printStackTrace(); }catch( ParseException
         * e) { e.printStackTrace(); }
         */
    }
}
