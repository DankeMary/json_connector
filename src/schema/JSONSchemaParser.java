package schema;
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
    //private static Map<String, Table> tables;
    private static List<Table> tables;
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
        //handleColumn(rootTable, "$id", "string", "");

        JSONObject properties = (JSONObject) schema.get("properties");

        parseProperties(rootTable, user, properties);
        
        for (Map.Entry<String, List<Column>> entry : tableColumns.entrySet()) {
            System.out.println("Table: " + entry.getKey());
            List<Column> cols = entry.getValue();
            for(Column c : cols)
                System.out.println("    " + c.getName());
        }     
    }
    
    /**
     * Проверяет, существует ли указанный путь и наличие столбца по нему
     * @param c      искомый столбец
     * @param schema JSON-схема
     * @param path   путь
     * @return       валидный/невалидный
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
     * Разбор пар в properties на столбцы и таблицы
     * 
     * @param parentTable таблица-владелец
     * @param user        пользователь-владелец
     * @param props       множество столбцов таблицы
     */
    private static void parseProperties(Table parentTable, User user, JSONObject props)
    {
        for (Object keyObj : props.keySet())
        {
            String key = (String) keyObj;
            JSONObject colData = (JSONObject) props.get(key);
            Column newCol;

            if (colData.containsKey("$ref"))
            {
                String path = (String) colData.get("$ref");
                String name = path.substring(path.lastIndexOf("/") + 1);                
                newCol = handleColumn(parentTable, name, findDef(path, (JSONObject)schema.get("definitions"), schema));
            }
            else
                newCol = handleColumn(parentTable, key, colData);
            
            if (newCol.getType().equals("object") || newCol.getType().equals("array"))
            {
                String name = key;
                if (colData.containsKey("$ref"))
                {
                    String path = (String) colData.get("$ref");
                    name = path.substring(path.lastIndexOf("/") + 1);
                    if (tableColumns.containsKey(name))
                        continue;
                    colData = findDef((String) colData.get("$ref"),(JSONObject)schema.get("definitions"), schema);                   
                }
                else if(newCol.getType().equals("array"))
                {
                    newCol.setType("string");
                    colData = (JSONObject)colData.get("items");
                }

                if (colData.get("type").equals("object"))
                {
                    Table newTable = handleTable(name, user, colData);                
                    parseProperties(newTable, user, (JSONObject) colData.get("properties"));
                }
            }
            
            /*if (newCol.getType().equals("object"))
            {
                String name = key;
                if (colData.containsKey("$ref"))
                {
                    String path = (String) colData.get("$ref");
                    name = path.substring(path.lastIndexOf("/") + 1);
                    //if (tables.containsKey(name))
                    if (tableColumns.containsKey(name))
                        continue;
                    colData = findDef((String) colData.get("$ref"),(JSONObject)schema.get("definitions"), schema);                   
                }
                Table newTable = handleTable(name, user, colData);
                //handleColumn(newTable, "$id", "string", "");
                
                parseProperties(newTable, user, (JSONObject) colData.get("properties"));
            }
            else if (newCol.getType().equals("array"))
            {
                newCol.setType("string"); //string of ids
                String name = key;
                if (colData.containsKey("$ref"))
                {
                    String path = (String) colData.get("$ref");
                    name = path.substring(path.lastIndexOf("/") + 1);
                    //if (tables.containsKey(name))
                    if (tableColumns.containsKey(name))
                        //break;
                        continue;
                    colData = findDef((String) colData.get("$ref"),(JSONObject)schema.get("definitions"), schema);                     
                }
                else 
                    colData = (JSONObject)colData.get("items");
                //type: array, array of arrays
                if (colData.get("type").equals("object"))
                {
                    Table newTable = handleTable(name, user, colData);                
                    parseProperties(newTable, user, (JSONObject) colData.get("properties"));
                }                
            }*/
        }
    }

    /**
     * Создает объект столбца, заполняет его данными и добавляет в список столбцов
     * 
     * @param parentTable таблица-владелец
     * @param name        имя столбца
     * @param colData     данные о столбце
     * @return            экземпляр столбца с данными
     */
    private static Column handleColumn(Table parentTable, String name, JSONObject colData)
    {
        Column newCol = new Column();
        name = name.trim().toLowerCase();
        newCol.setName(name);
        newCol.setType(((String)colData.get("type")).trim().toLowerCase());
        newCol.setComment((String) colData.get("description"));
        newCol.setTable(parentTable);
        if (!columns.containsKey(name))        
            columns.put(name, new LinkedList<Column>());
        columns.get(name).add(newCol);
     
        tableColumns.get(parentTable.getName()).add(newCol);
        return newCol;
    }

    /**
     * Создает объект столбца, заполняет его данными и добавляет в список столбцов
     * @param parentTable таблица-владелец
     * @param name        имя столбца
     * @param type        тип данных столбца
     * @param comment     комментарий
     * @return            экземпляр столбца с данными
     */
    /*private static Column handleColumn(Table parentTable, String name, String type, String comment)
    {
        Column newCol = new Column();
        newCol.setName(name);
        newCol.setTable(parentTable);
        newCol.setType(type);
        newCol.setComment(comment);
        if (!columns.containsKey(name))        
            columns.put(name, new LinkedList<Column>());
        columns.get(name).add(0, newCol);
     
        tableColumns.get(parentTable.getName()).add(newCol);        
        return newCol;
    }*/
    
    /**
     * Создает объект таблицы, заполняет его данными и добавляет в список таблиц
     * 
     * @param name    имя таблицы
     * @param user    пользователь-владелец
     * @param tabData данные о таблице
     * @return        экземпляр таблицы с данными
     */
    private static Table handleTable(String name, User user, JSONObject tabData)
    {
        /*if(tables.containsKey(name))
             return tables.get(name);
        else
        {*/
            name = name.trim().toLowerCase();
            Table newTable = new Table();
            newTable.setName(name);
            newTable.setType(Table.TYPE_TABLE);
            newTable.setOwner(user);
            newTable.setComment((String) tabData.get("description"));
            //tables.put(name, newTable);
            tables.add(0, newTable);
            tableColumns.put(name, new LinkedList<Column>());           
            
            return newTable;
        //}     
    }

    /**
     * Поиск значения ключа по заданной ссылке
     * 
     * @param path ссылка
     * @param defs определения столбцов
     * @param obj  текущая область поиска
     * @return     значение ключа по ссылке
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
     * Производит построение всех возможных путей до столбца с заданным родителем
     * 
     * @param schema JSON-схема
     * @param c      столбец
     * @param paths  список возможных путей
     * @return       список возможных путей
     */
    public static List<String> buildColumnPaths(JSONSchema objSchema, Column c, List<String> paths)
    {
       initSchema(objSchema);
       buildPath(objSchema, "", c, paths);
       return paths;
    }
    
    /**
     * Создает словарь путей, столбцы из одинаковой таблицы имеют один и тот же ключ
     * 
     * @param objSchema JSON-схема
     * @param cols      искомые столбцы
     */
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
     * Рекурсивно строит путь к столбцу
     * 
     * @param schema JSON-схема
     * @param path   текущий путь
     * @param c      текущий столбец
     * @param paths  список возможных путей
     * @return       путь к столбцу
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
