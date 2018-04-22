package utils;


import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


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
        int dotPos = fname.lastIndexOf(".");
        if (dotPos > 0)
            fname = fname.substring(0, dotPos);
        return fname;
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
            System.out.println(e.getMessage());
            throw new RuntimeException();
        }
        catch (ParseException e)
        {
            System.out.println(e.getMessage());
            throw new RuntimeException();
        }
    }
}
