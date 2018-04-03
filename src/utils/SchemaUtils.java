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

    /**
     * Áåðåò èìÿ òàáëèöû èç ïóòè ê ôàéëó ñõåìû
     * 
     * @param path ïóòü ê ôàéëó
     * @return     èìÿ òàáëèöû
     */
    public static String getTableName(String path)
    {
        Path p = Paths.get(path);
        String fname = p.getFileName().toString();
        int pos = fname.lastIndexOf(".");
        if (pos > 0)
            fname = fname.substring(0, pos);
        return fname;
    }

    /**
     * Ïðåîáðàçóåò íàçâàíèÿ ê lowerCamelCase
     * 
     * @param str íàçâàíèå
     * @return    ôîðìàòèðîâàííîå íàçâàíèå
     */
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

    /**
     * Ñîçäàåò îáúåêò ñòîëáöà òàáëèöû, çàïîëíÿåò åãî äàííûìè è äîáàâëÿåò â
     * ñïèñîê ñòîëáöîâ
     * 
     * @param parentTable òàáëèöà-âëàäåëåö
     * @param columns     ñïèñîê ñòîëáöîâ
     * @param name        íàçâàíèå ñòîëáöà
     * @param colData     äàííûå î ñòîëáöå
     * @return            ýêçåìïëÿð ñòîëáöà ñ äàííûìè
     */
    public static Column handleColumn(Table parentTable, List<Column> columns,
        String name, JSONObject colData)
    {
        Column newCol = new Column();
        newCol.setName(formatName(name));
        newCol.setType((String) colData.get("type"));
        newCol.setComment((String) colData.get("description"));
        newCol.setTable(parentTable);
        columns.add(newCol);
        return newCol;
    }

    /**
     * Ñîçäàåò îáúåêò òàáëèöû, çàïîëíÿåò åãî äàííûìè è äîáàâëÿåò â ñïèñîê òàáëèö
     * 
     * @param name    èìÿ òàáëèöû
     * @param user    ïîëüçîâàòåëü-âëàäåëåö
     * @param tabData äàííûå î òàáëèöå
     * @param tables  ñïèñîê òàáëèö
     * @return        ýêçåìïëÿð òàáëèöû ñ äàííûìè
     */
    public static Table handleTable(String name, User user, JSONObject tabData,
        List<Table> tables)
    {
        Table newTable = new Table();
        newTable.setName(formatName(name));
        newTable.setType(Table.TYPE_TABLE);
        newTable.setOwner(user);
        newTable.setComment((String) tabData.get("description"));
        tables.add(newTable);
        return newTable;
    }

    // props -> infinite loop
    /**
     * Ïîèñê çíà÷åíèÿ êëþ÷à ïî çàäàííîé ññûëêå
     * 
     * @param path ïóòü
     * @param obj  îáëàñòü ïîèñêà
     * @return     çíà÷åíèå êëþ÷à ïî ññûëêå
     */
    public static JSONObject findDef(String path, JSONObject obj)
    {
        if (path == null || path.isEmpty())
            return null;
        if (path.indexOf("/") == -1)
            return (JSONObject) obj.get(path);
        if (path.substring(0, 1).equals("#"))
            return findDef(path.substring(path.indexOf("/") + 1), obj);
        else
            return findDef(path.substring(path.indexOf("/") + 1),
                (JSONObject) obj.get(path.substring(0, path.indexOf("/"))));
    }

    /**
     * Ðàçáîð ïàð â properties íà ñòîëáöû è òàáëèöû
     * 
     * @param schema      êîðíåâàÿ JSON-ñõåìà
     * @param parentTable òàáëèöà-âëàäåëåö
     * @param tables      ñïèñîê òàáëèö
     * @param columns     ñïèñîê ñòîëáöîâ
     * @param user        ïîëüçîâàòåëü-âëàäåëåö
     * @param props       ìíîæåñòâî ñòîëáöîâ òàáëèöû
     * @param defs        ìíîæåñòâî îïðåäåëåíèé îáúåêòîâ ñõåìû
     */
    public static void parseSchemaProperties(JSONObject schema,
        Table parentTable, List<Table> tables, List<Column> columns, User user,
        JSONObject props, JSONObject defs)
    {
        for (Object keyObj : props.keySet())
        {
            String key = (String) keyObj;
            JSONObject colData = (JSONObject) props.get(key);
            Column newCol;

            if (colData.containsKey("$ref"))
                newCol = handleColumn(parentTable, columns, key,
                    findDef((String) colData.get("$ref"), schema));
            else
                newCol = handleColumn(parentTable, columns, key, colData);

            if (newCol.getType().equals("object"))
            {
                if (colData.containsKey("$ref"))
                    colData = findDef((String) colData.get("$ref"), schema);
                Table newTable = handleTable(key, user, colData, tables);

                parseSchemaProperties(schema, newTable, tables, columns, user,
                    (JSONObject) colData.get("properties"), defs);
            }
        }
    }

    /**
     * Çàãðóçêà JSON-ñõåìû èç ôàéëà
     * 
     * @param path ïóòü ê ôàéëó
     * @return     JSON-ñõåìà
     */
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

    /**
     * Ðàçáîð JSON-ñõåìû íà òàáëèöû è ñòîëáöû
     * 
     * @param path ïóòü ê ôàéëó JSON-ñõåìû
     */
    public static void parseJsonSchema(String path)
    {
        JSONObject schema = getJson(path);

        LinkedList<Table> tables = new LinkedList<Table>();
        LinkedList<Column> columns = new LinkedList<Column>();

        User user = new User();
        user.setName(USER_NAME);

        Table rootTable = handleTable(getTableName(path), user, schema, tables);

        JSONObject properties = (JSONObject) schema.get("properties");
        JSONObject defs = (JSONObject) schema.get("definitions");

        parseSchemaProperties(schema, rootTable, tables, columns, user,
            properties, defs);
        /*
         * for (Table t : tables) System.out.print(t.getName() + " ");
         * System.out.println(); for (Column c : columns)
         * System.out.print(c.getName() + " ");
         */
    }
}
