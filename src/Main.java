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
        /*JSONSchema ha = new JSONSchema();
        Table haha = new Table();
        haha.setName("la");
        System.out.println(ha.getColumns(haha));*/
        String s_path = "e:\\address.json";
        String d_path = "e:\\try.json";
        TestClass.runTest(s_path, d_path);
        System.out.println("Succeeded1");    
        //JSONSchema s =  new JSONSchema(SchemaUtils.getTableName(s_path), SchemaUtils.getJson(s_path));
        
        //List<String> paths = new LinkedList<String>();

        //JSONObject obj = SchemaUtils.getJson("e:\\test2.json");
         //JSONObject obj = SchemaUtils.getJson("e:\\try.json");
        
        //SQLiteHandler test = new SQLiteHandler(s.getName());
        //test.uploadData(obj);
        //Table tTab = new Table();
        //tTab.setName("haha");
        //test.getData(tTab, (Column)s.getTableColumns().get(tTab.getName()).toArray()[0], (Column)s.getTableColumns().get(tTab.getName()).toArray()[2]);
        //test.getData(tTab);
        //test.deleteDatabase();   
        //System.out.println("Succeeded");             
    }
}
