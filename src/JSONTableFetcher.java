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
     * Берет значение из столбца по указанному пути
     * @param name имя столбца
     * @param path путь к столбцу
     * @param obj  JSONобъект
     * @return     значение столбца
     */
    public static String getToColumn(String name, String path, JSONObject obj)
    {
        JSONObject curr = (JSONObject)obj.clone();
        String[] nodes = path.replaceFirst("^/", "").split("/", 0);
        for(String s : nodes)
            curr = (JSONObject)curr.get(s);
        return (String)curr.get(name);
    }

    /**
     * Производит построение всех возможных путей до столбца с заданным родителем
     * @param schema JSON-схема
     * @param c      столбец
     * @param paths  список возможных путей
     * @return       список возможных путей
     */
    public static List<String> buildPaths(JSONSchema schema, Column c, List<String> paths)
    {
       buildPath(schema, "", c, paths);
       return paths;
    }
    
    /**
     * Рекурсивно строит путь к столбцу
     * @param schema JSON-схема
     * @param path   текущий путь
     * @param c      текущий столбец
     * @param paths  список возможных путей
     * @return       путь к столбцу
     */
    public static String buildPath(JSONSchema schema, String path, Column c, List<String> paths)
    {
        Table parentTable = c.getTable(); 
        List<Column> possible = schema.getColumns(parentTable.getName());
        
        StringBuilder currPath = new StringBuilder("" + path); 
        if(!path.equals(""))
            currPath.insert(0, c.getName());
        currPath.insert(0, "/");
        if (possible == null)
        {
            paths.add(currPath.toString());
            return ""; 
        }
        else
            for(Column col : possible)
                currPath.insert(0, buildPath(schema, currPath.toString(), col, paths)); 
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
