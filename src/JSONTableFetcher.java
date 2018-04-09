import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

import model.Column;
import model.Table;

/**
 * @author MM
 *
 */
public class JSONTableFetcher implements TableFetcher
{
    public List<Table> getTables(JSONSchema schema)
    {
        return schema.getListedTables();
    }
    
    public List<Column> getColumns(JSONSchema schema)
    {
        return schema.getListedColumns();
    }
       
    /**
     * ����� �������� �� ������� �� ���������� ����
     * @param name ��� �������
     * @param path ���� � �������
     * @param obj  JSON������
     * @return     �������� �������
     */
    public static String getColumnData(String name, String path, JSONObject obj)
    {
        JSONObject curr = (JSONObject)obj.clone();
        if(!path.trim().equals("/"))
        {
            String[] nodes = path.replaceFirst("^/", "").split("/", -1);
            for(String s : nodes)
                curr = (JSONObject)curr.get(s);
        }
        return (String)curr.get(name);
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
