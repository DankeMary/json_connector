import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

import model.Column;
import model.Table;

public class JSONTableFetcher implements TableFetcher
{
    public List<Table> getTables(JSONSchema schema)
    {
        return null;
        //return schema.getTables();
    }
    public List<Column> getColumns(JSONSchema schema)
    {
        //return schema.getColumns();
        return null;
    }
    public List<Column> getAllColumns(JSONSchema schema)
    {
        //return schema.getColumns();
        return null;
    }
    //+
    public static String getToColumn(String name, String path, JSONObject obj)
    {
        JSONObject curr = (JSONObject)obj.clone();
        String[] nodes = path.split("/");
        for(String s : nodes)
        {
            curr = (JSONObject)curr.get(s);
        }
        return (String)curr.get(name);
    }
    //+
    public List<String> buildPaths(JSONSchema schema, Column c, List<String> paths)
    {
       buildPath(schema, new StringBuilder(""), c, paths);
       return paths;
    }
    //+
    public String buildPath(JSONSchema schema, StringBuilder sb, Column c, List<String> paths)
    {
        Table parentTable = c.getTable(); 
        List<Column> possible = schema.getColumns(parentTable.getName());

        if (possible == null)
        {
            sb.insert(0, c.getName());
            paths.add(sb.toString());
            sb.setLength(0);            
            return ""; 
        }
        else
            for(Column col : possible)
            {
                //sb.insert(0, c.getName());
                sb.insert(0, "/");
                sb.insert(0, buildPath(schema, sb, col, paths));                
            }
        return "";
    }
    
    /*public List<String> getColumns(JSONSchema schema, JSONObject json, {Table t,} List<String> cols)
    {
        Map<String, List<String>> paths = new HashMap<String, List<String>>();
        
        for(String s : cols)
        {
            //buildpath
            List<Column> colList = schema.getColumns(s);
            
            for(Column c : colList)
            {
                paths.put(s, buildPath(c));
            }
        }
        //for each jsonobject in jsonarray get cols values
        return null;
    }*/
    
    /*public List<Column> getColumns(JSONSchema schema, Table table, String... columns)
    {
        List<String> params = (Array)columns
        List<Column> cols = schema.getColumns(table);
        List<Column> res = new LinkedList<Column>();
        
        
        for(String c : columns)
        {
            if (cols.contains(c))
                res.add(cols.indexOf())
        }
        for(Column c : schema.getColumns(table))
            if (c.getName() in columns)
        return null;
    }*/

    public void getAllData(JSONSchema schema, JSONObject json)
    {
        //validate json
        
    }

    public void getData(Column... columns)
    {
        
        
    }
    
}
