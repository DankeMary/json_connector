import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.List;

import org.json.simple.JSONObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

import database.SQLiteHandler;
import model.Column;
import model.Table;
import schema.JSONSchema;
import schema.JSONSchemaParser;
import utils.SchemaUtils;

public class Main
{
    public static void main(String args[])
    {
        /*StringBuilder str = new StringBuilder("lala");
        SchemaUtils.changeString(str);
        System.out.println(str);*/
        
        
        
        /*Gson gson = new GsonBuilder()
                .setLenient()
                .create();*/
        /*JsonParser parser = new JsonParser();
        try
        {
            JsonReader reader = new JsonReader(new FileReader("e:\\nan.json"));
            reader.setLenient(true);
            JsonElement element = parser.parse(reader);
            JsonObject num = element.getAsJsonObject();
            System.out.println(num.get("num"));
            //JSONObject obj = SchemaUtils.getJson("e:\\nan.json");
            System.out.println("Succeeded 1");
        }
        catch (FileNotFoundException e) {}*/
        
        
        
        
        //JsonParser parser = new JsonParser();
        //JsonElement element = parser.parse("e:\\nan.json");
        
       
        
        
        
        String s_path = "e:\\schema.json";    
        JSONSchema s =  new JSONSchema(SchemaUtils.getTableName(s_path), SchemaUtils.getJson(s_path));
        
        List<String> paths = new LinkedList<String>();

        //Column pp = s.getColumns("first-name").get(0);
        
        //JSONObject obj = SchemaUtils.getJson("e:\\test2.json");
         JSONObject obj = SchemaUtils.getJson("e:\\data.json");
        /*JSONObject obj = SchemaUtils.getJson("e:\\nan.json");
        
        obj.get("num");
        System.out.println(obj.get("num"));*/
        
        SQLiteHandler test = new SQLiteHandler(s);
        test.uploadData(obj);
        Table tTab = new Table();
        tTab.setName("block_size");
        //test.getData(tTab, (Column)s.getTableColumns().get(tTab.getName()).toArray()[0], (Column)s.getTableColumns().get(tTab.getName()).toArray()[2]);
        test.getData(tTab);
        System.out.println("Succeeded");
        //test.deleteDatabase();
        
        /*for(Table t: s.getTables())
            test.walkThroughAndLoad(t.getName(), null, null);*/
        
        //JSONSchemaParser.buildColumnPaths(s, s.getColumns("first-name").get(0), paths);
        //JSONSchemaParser.buildAndMatchAllPaths(s, s.getColumns("first-name").get(0), s.getColumns("street-address").get(0));
        /*for(String path : paths)
            if(JSONSchemaParser.checkPath(s.getColumns("first-name").get(0), s, path))
                System.out.println(path + "   " + JSONTableFetcher.getColumnData("first-name", path, obj));
            else System.out.println("failed");*/
        /*for(Table t: s.getTables())
            System.out.println(t.getName());*/
        
        /*for(Table t: s.getTables())
            System.out.println(JSONSchemaParser.createTable(t));*/
        
        
              
    }
}
