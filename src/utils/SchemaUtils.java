package utils;


import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.JSONStreamAware;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import model.Column;
import model.Table;
import model.User;



public class SchemaUtils
{
    public static final String USER_NAME = "JSON";

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
     * Создает объект столбца таблицы, заполняет его данными и добавляет 
     * в список столбцов
     * 
     * @param parentTable таблица-владелец
     * @param columns     список столбцов
     * @param name        название столбца
     * @param colData     данные о столбце
     * @return            экземпляр столбца с данными
     */
    public static Column handleColumn(Table parentTable, /*List<Column> columns*/Map<String, List<Column>> tableColumns,
        String name, JSONObject colData)
    {
        Column newCol = new Column();
        newCol.setName(name);
        newCol.setType((String) colData.get("type"));
        newCol.setComment((String) colData.get("description"));
        newCol.setTable(parentTable);
        //columns.add(newCol);
        tableColumns.get(parentTable.getName()).add(newCol);
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
        /*List<Table>*/Map<String, Table> tables, Map<String, List<Column>> tableColumns)
    {
        Table newTable = new Table();
        newTable.setName(name);
        newTable.setType(Table.TYPE_TABLE);
        newTable.setOwner(user);
        newTable.setComment((String) tabData.get("description"));
        tables.put(name, newTable);
        tableColumns.put(name, new LinkedList<Column>());
        return newTable;
    }

    // props -> infinite loop
    /**
     * Поиск значения ключа по заданной ссылке
     * 
     * @param path ссылка
     * @param obj  область поиска
     * @return     значение ключа по ссылке
     */
    public static JSONObject findDef(String path, JSONObject defs, JSONObject obj)
    {
        if (path == null || path.isEmpty())
            return null;
        if (path.indexOf("/") == -1)
            if(obj.containsKey("$ref"))
                return findDef((String) obj.get("$ref"), defs, defs);
            else
                return (JSONObject) obj.get(path);
        if (path.substring(0, 1).equals("#"))
            return findDef(path.substring(path.indexOf("/") + 1), defs, obj);
        else
            return findDef(path.substring(path.indexOf("/") + 1), defs,
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
        Table parentTable, /*List<Table>*/Map<String, Table> tables, /*List<Column> columns*/Map<String, List<Column>> tableColumns, 
        User user, JSONObject props, JSONObject defs)
    {
        for (Object keyObj : props.keySet())
        {
            String key = (String) keyObj;
            JSONObject colData = (JSONObject) props.get(key);
            Column newCol;

            if (colData.containsKey("$ref"))
                newCol = handleColumn(parentTable, /*columns*/tableColumns, key,
                    findDef((String) colData.get("$ref"), (JSONObject)schema.get("definitions"), schema));
            else
                newCol = handleColumn(parentTable, /*columns*/tableColumns, key, colData);
            
            if (newCol.getType().equals("object"))
            {
                if (colData.containsKey("$ref"))
                    colData = findDef((String) colData.get("$ref"),(JSONObject)schema.get("definitions"), schema);
                Table newTable = handleTable(key, user, colData, tables, tableColumns);

                parseSchemaProperties(schema, newTable, tables, /*columns*/tableColumns, user,
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

    public List<String> getColumns(Table currTable, Map<String, Table> tables, JSONObject json, Table t/*, List<String> cols*/)
    {
        List<String> cols = new LinkedList<String>();
        cols.add("x"); 
        cols.add("y");
        t = tables.get("extent");
        
        
        //for each jsonobject in jsonarray get cols values
        return null;
    }
}
