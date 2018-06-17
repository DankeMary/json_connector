import java.util.LinkedList;
import java.util.List;

import test.TestClass;
import utility.Utility;


public class Main
{
    public static void main(String args[])
    {
        //String schema_path = "some_json_path";
        //String data_path = "some_schema_path";

        //TestClass.runTest(schema_path, data_path);
        
        Utility utility = new Utility();
        //utility.uploadSchema("C:\\Users\\MM\\Desktop\\schema.json"); //sample_schema.json");
        utility.uploadSchema("C:\\Users\\MM\\Desktop\\sample_schema.json");
        utility.printTablesColumns();
        
        //utility.uploadData("C:\\Users\\MM\\Desktop\\data.json"); //sample_data.json");
        utility.uploadData("C:\\Users\\MM\\Desktop\\sample_data.json");
        
        utility.createDatabase();
        
        utility.fillDatabase();
        
        /*List<String> ss = new LinkedList<String>();
        ss.add("y");*/
        
        for(String s : utility.getAllData("quality"))
            System.out.println(s);
        
        //utility.getAllData("center");
    }
}
