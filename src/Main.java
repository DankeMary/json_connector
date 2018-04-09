import java.util.LinkedList;
import java.util.List;

import org.json.simple.JSONObject;

import model.Column;
import model.Table;
import utils.SchemaUtils;

public class Main
{
    public static void main(String args[])
    {
        String s_path = "e:\\address.json";    
        JSONSchema s =  new JSONSchema(SchemaUtils.getTableName(s_path), SchemaUtils.getJson(s_path));
        
        List<String> paths = new LinkedList<String>();

        //Column pp = s.getColumns("first-name").get(0);
        
        JSONObject obj = SchemaUtils.getJson("e:\\test2.json");
        
        JSONSchemaParser.buildColumnPaths(s, s.getColumns("extended-address").get(0), paths);
        JSONSchemaParser.buildAndMatchAllPaths(s, s.getColumns("first-name").get(0), s.getColumns("street-address").get(0));
        for(String path : paths)
            if(JSONSchemaParser.checkPath(s.getColumns("extended-address").get(0), s, path))
                System.out.println(path + "   " + JSONTableFetcher.getColumnData("extended-address", path, obj));
            else System.out.println("failed");
        
              
    }
}
