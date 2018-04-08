import java.util.LinkedList;
import java.util.List;

import org.json.simple.JSONObject;

import model.Column;
import utils.SchemaUtils;

public class Main
{
    public static void main(String args[])
    {
        String s_path = "e:\\address.json";    
        //SchemaUtils.parseJsonSchema(s_path);
        JSONSchema s =  new JSONSchema(SchemaUtils.getTableName(s_path), SchemaUtils.getJson(s_path));
        //System.out.println(s.getName());
        //Column p = s.getColumns("street-address").get(0);
        List<String> paths = new LinkedList<String>();
       // s.buildPath(new StringBuilder(""), p, paths);
        /*for(String str: paths)
            System.out.println(str);*/
        
        Column pp = s.getColumns("first-name").get(0);
        
        JSONObject obj = SchemaUtils.getJson("e:\\test2.json");
        s.buildPath(new StringBuilder(""), pp, paths);
        for(String path : paths)
            System.out.println(JSONTableFetcher.getToColumn("first-name", path, obj));
    }
}
