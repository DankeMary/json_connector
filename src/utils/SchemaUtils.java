package utils;


import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import model.Column;
import model.Table;
import schema.JSONSchema;


public class SchemaUtils
{
    public static final String USER_NAME = "JSON";

    /**
     * Берет имя таблицы из пути к файлу схемы
     * 
     * @param path путь к файлу
     * @return имя таблицы
     */
    public static String getTableName(String path)
    {
        Path p = Paths.get(path);
        String fname = p.getFileName().toString();
        int dotPos = fname.lastIndexOf(".");
        if (dotPos > 0)
            fname = fname.substring(0, dotPos);
        return fname;
    }

    /**
     * Загрузка JSON-схемы из файла
     * 
     * @param path путь к файлу
     * @return JSON-схема
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
            System.out.println(e.getMessage());
            throw new RuntimeException();
        }
        catch (ParseException e)
        {
            System.out.println(e.getMessage());
            throw new RuntimeException();
        }
    }
    
    /**
     * Печать названий всех таблиц и их столбцов 
     * @param schema JSON-схема
     */
    public void printTablesColumns(JSONSchema schema)
    {
        for (Map.Entry<String, List<Column>> entry : schema.getTableColumns().entrySet()) {
             System.out.println("Table: " + entry.getKey());
             List<Column> cols = entry.getValue();
             for(Column c : cols)
                 System.out.println("    " + c.getName());
         } 
    }   
    
    /**
     * Проверяет столбцы на принадлежность таблице
     * 
     * @param table таблица
     * @param columns столбцы
     * @return true - все принадлежат, false - хотя бы 1 не принадлежит
     */
    public static boolean checkColumns(Table table, List<Column> columns)
    {
        for (Column c : columns)
        {
            if (!table.getName().equals(c.getTable().getName()))
                return false;
        }
        return true;
    }
}
