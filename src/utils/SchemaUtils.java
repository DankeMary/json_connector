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
     * Берет имя таблицы из пути к файлу схемы
     * 
     * @param path путь к файлу
     * @return     имя таблицы
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
     * Преобразует названия к lowerCamelCase
     * 
     * @param str название
     * @return    форматированное название
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
     * Создает объект столбца таблицы, заполняет его данными и добавляет в
     * список столбцов
     * 
     * @param parentTable таблица-владелец
     * @param columns     список столбцов
     * @param name        название столбца
     * @param colData     данные о столбце
     * @return            экземпляр столбца с данными
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
     * Создает объект таблицы, заполняет его данными и добавляет в список таблиц
     * 
     * @param name    имя таблицы
     * @param user    пользователь-владелец
     * @param tabData данные о таблице
     * @param tables  список таблиц
     * @return        экземпляр таблицы с данными
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
     * Поиск значения ключа по заданной ссылке
     * 
     * @param path путь
     * @param obj  область поиска
     * @return     значение ключа по ссылке
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
     * Разбор пар в properties на столбцы и таблицы
     * 
     * @param schema      корневая JSON-схема
     * @param parentTable таблица-владелец
     * @param tables      список таблиц
     * @param columns     список столбцов
     * @param user        пользователь-владелец
     * @param props       множество столбцов таблицы
     * @param defs        множество определений объектов схемы
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
     * Загрузка JSON-схемы из файла
     * 
     * @param path путь к файлу
     * @return     JSON-схема
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
     * Разбор JSON-схемы на таблицы и столбцы
     * 
     * @param path путь к файлу JSON-схемы
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
