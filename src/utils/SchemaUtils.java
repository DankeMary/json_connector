package utils;

import java.nio.file.Path;
import java.nio.file.Paths;

public class SchemaUtils
{
    public static String firstUpperCase(String word)
    {
        if(word == null || word.isEmpty()) 
            return "";
        return word.substring(0, 1).toUpperCase() + word.substring(1);
    }
    
    public static String getTableName(String path)
    {
        Path p = Paths.get(path);
        String fileName = p.getFileName().toString();
        String[] parts = fileName.split(".json");
        return parts[0];
        
        /*String fname = file.getName();
        int pos = fname.lastIndexOf(".");
        if (pos > 0) {
            fname = fname.substring(0, pos);
        }*/
    }
}
