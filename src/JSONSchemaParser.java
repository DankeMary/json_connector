import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

import model.Column;
import model.Table;
import model.User;

public class JSONSchemaParser
{
    public static final String USER_NAME = "JSON";
    
    private static JSONObject schema;
    private static String name;
    private static Map<String, Table> tables;
    private static Map<String, List<Column>> columns;
    private static Map<String, List<Column>> tableColumns;
    
    private static void initSchema(JSONSchema objSchema)
    {
        schema = objSchema.getSchema();
        name = objSchema.getName();
        tables = objSchema.getTables();
        columns = objSchema.getColumns();
        tableColumns = objSchema.getTableColumns();
    }
    public static void parse(JSONSchema objSchema)
    {   
        initSchema(objSchema);
        
        User user = new User();
        user.setName(USER_NAME);

        Table rootTable = handleTable(name, user, schema);

        JSONObject properties = (JSONObject) schema.get("properties");

        parseProperties(rootTable, user, properties);
        
        /*for (Map.Entry<String, List<Column>> entry : tableColumns.entrySet()) {
            System.out.println("Table: " + entry.getKey());
            List<Column> cols = entry.getValue();
            for(Column c : cols)
                System.out.println("    " + c.getName());
        }   */      
    }
    
    /**
     * ���������, ���������� �� ��������� ���� � ������� ������� �� ����
     * @param c      ������� �������
     * @param schema JSON-�����
     * @param path   ����
     * @return       ��������/����������
     */
    public static boolean checkPath(Column c, JSONSchema objSchema, String path)
    {
        initSchema(objSchema);
        JSONObject curr = (JSONObject) schema.get("properties");  
        if(!path.trim().equals("/"))
        {
            String[] nodes = path.replaceFirst("^/", "").split("/", 0);
        
            for(String s : nodes)
            {
                if(curr.containsKey(s))
                {
                    curr = (JSONObject)curr.get(s);
                    if (curr.containsKey("$ref"))                                        
                        curr = findDef((String)curr.get("$ref"), (JSONObject)schema.get("definitions"), schema);
                    if (curr.get("type").equals("object"))
                        curr = (JSONObject)curr.get("properties");
                }
                else 
                    return false;
            }
        }
        return curr.containsKey(c.getName());
    }
    
    /**
     * ������ ��� � properties �� ������� � �������
     * 
     * @param schema      �������� JSON-�����
     * @param parentTable �������-��������
     * @param tables      ������ ������
     * @param columns     ������ ��������
     * @param user        ������������-��������
     * @param props       ��������� �������� �������
     * @param defs        ��������� ����������� �������� �����
     */
    private static void parseProperties(Table parentTable, User user, JSONObject props)
    {
        for (Object keyObj : props.keySet())
        {
            String key = (String) keyObj;
            JSONObject colData = (JSONObject) props.get(key);
            Column newCol;

            if (colData.containsKey("$ref"))
                newCol = handleColumn(parentTable, key, findDef((String) colData.get("$ref"), (JSONObject)schema.get("definitions"), schema));
            else
                newCol = handleColumn(parentTable, key, colData);
            
            if (newCol.getType().equals("object"))
            {
                if (colData.containsKey("$ref"))
                    colData = findDef((String) colData.get("$ref"),(JSONObject)schema.get("definitions"), schema);
                Table newTable = handleTable(key, user, colData);

                parseProperties(newTable, user, (JSONObject) colData.get("properties"));
            }
        }
    }

    private static Column handleColumn(Table parentTable, String name, JSONObject colData)
    {
        Column newCol = new Column();
        newCol.setName(name);
        newCol.setType((String) colData.get("type"));
        newCol.setComment((String) colData.get("description"));
        newCol.setTable(parentTable);
        if (!columns.containsKey(name))        
            columns.put(name, new LinkedList<Column>());
        columns.get(name).add(newCol);
     
        tableColumns.get(parentTable.getName()).add(newCol);
        return newCol;
    }

    /**
     * ������� ������ �������, ��������� ��� ������� � ��������� � ������ ������
     * 
     * @param name    ��� �������
     * @param user    ������������-��������
     * @param tabData ������ � �������
     * @return        ��������� ������� � �������
     */
    private static Table handleTable(String name, User user, JSONObject tabData)
    {
        if(tables.containsKey(name))
             return tables.get(name);
        else
        {
            Table newTable = new Table();
            newTable.setName(name);
            newTable.setType(Table.TYPE_TABLE);
            newTable.setOwner(user);
            newTable.setComment((String) tabData.get("description"));
            tables.put(name, newTable);
            tableColumns.put(name, new LinkedList<Column>());
            return newTable;
        }     
    }

    /**
     * ����� �������� ����� �� �������� ������
     * 
     * @param path ������
     * @param obj  ������� ������
     * @return     �������� ����� �� ������
     */
    private static JSONObject findDef(String path, JSONObject defs, JSONObject obj)
    {
        if (path == null || path.isEmpty())
            return null;
        if (path.indexOf("/") == -1)
            if(obj.containsKey("$ref"))
                return findDef((String) obj.get("$ref"), defs, defs);
            else
                return (JSONObject) obj.get(path);
        if (path.substring(0, 1).equals("#"))
            return findDef(path.substring(path.indexOf("/") + 1), defs, obj);
        else
            return findDef(path.substring(path.indexOf("/") + 1), defs,
                (JSONObject) obj.get(path.substring(0, path.indexOf("/"))));
    }
    
    /**
     * ���������� ���������� ���� ��������� ����� �� ������� � �������� ���������
     * @param schema JSON-�����
     * @param c      �������
     * @param paths  ������ ��������� �����
     * @return       ������ ��������� �����
     */
    public static List<String> buildColumnPaths(JSONSchema objSchema, Column c, List<String> paths)
    {
       initSchema(objSchema);
       buildPath(objSchema, "", c, paths);
       return paths;
    }
    
    public static /*Map<String, List<Column>>*/void buildAndMatchAllPaths(JSONSchema objSchema, Column... cols)
    {
        Map<String, List<Column>> matchedPaths = new HashMap<String, List<Column>>();
        for(Column c : cols)
        {
            List<String> colPaths = new LinkedList<String>();
            buildColumnPaths(objSchema, c, colPaths);
            for(String s : colPaths)
            {
                if(!matchedPaths.containsKey(s))
                    matchedPaths.put(s, new LinkedList<Column>());
                matchedPaths.get(s).add(c);
            }
        }        
    }
    
    /**
     * ���������� ������ ���� � �������
     * @param schema JSON-�����
     * @param path   ������� ����
     * @param c      ������� �������
     * @param paths  ������ ��������� �����
     * @return       ���� � �������
     */
    private static String buildPath(JSONSchema objSchema, String path, Column c, List<String> paths)
    {
        Table parentTable = c.getTable(); 
        List<Column> possible = objSchema.getColumns(parentTable.getName());
        
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
                currPath.insert(0, buildPath(objSchema, currPath.toString(), col, paths)); 
        return "";
    }
}
