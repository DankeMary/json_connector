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

    public static void changeString(StringBuilder str)
    {
        
        str.append("  LALALALALA");
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
}
