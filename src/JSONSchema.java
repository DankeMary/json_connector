import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

import model.Column;
import model.Table;
import model.User;

public class JSONSchema
{    
    public static final String USER_NAME = "JSON";
    
    private String name;
    private JSONObject schema;
    private Map<String, Table> tables;
    private Map<String, List<Column>> columns;
    private Map<String, List<Column>> tableColumns;
    
    public void setColumns(Map<String, List<Column>> columns)
    {
        this.columns = columns;
    }   

    public JSONSchema()
    {
        name = "";
        schema = null;
        tables = null;
        columns = null;
        tableColumns = null;
    }
    
    public JSONSchema(String name, JSONObject objSchema)
    {
        this.name = name; 
        schema = objSchema;
        tables = new HashMap<String, Table>();
        columns = new HashMap<String, List<Column>>();
        tableColumns = new HashMap<String, List<Column>>();
        parseJsonSchema();
    }
    
    public void parseJsonSchema()
    {       
        User user = new User();
        user.setName(USER_NAME);

        Table rootTable = handleTable(name, user, schema, tables, tableColumns);

        JSONObject properties = (JSONObject) schema.get("properties");
        JSONObject defs = (JSONObject) schema.get("definitions");

        parseSchemaProperties(schema, rootTable, tables, columns, tableColumns, user,
            properties, defs);
        
        for (Map.Entry<String, List<Column>> entry : tableColumns.entrySet()) {
            System.out.println("Table: " + entry.getKey());
            List<Column> cols = entry.getValue();
            for(Column c : cols)
                System.out.println("    " + c.getName());
        }         
    }
    
    /**
     * Разбор пар в properties на столбцы и таблицы
     * 
     * @param schema      корневая JSON-схема
     * @param parentTable таблица-владелец
     * @param tables      список таблиц
     * @param columns     список столбцов
     * @param user        пользователь-владелец
     * @param props       множество столбцов таблицы
     * @param defs        множество определений объектов схемы
     */
    public static void parseSchemaProperties(JSONObject schema,
        Table parentTable, /*List<Table>*/Map<String, Table> tables, Map<String, List<Column>> columns, Map<String, List<Column>> tableColumns, 
        User user, JSONObject props, JSONObject defs)
    {
        for (Object keyObj : props.keySet())
        {
            String key = (String) keyObj;
            JSONObject colData = (JSONObject) props.get(key);
            Column newCol;

            if (colData.containsKey("$ref"))
                newCol = handleColumn(parentTable, columns, tableColumns, key,
                    findDef((String) colData.get("$ref"), (JSONObject)schema.get("definitions"), schema));
            else
                newCol = handleColumn(parentTable, columns, tableColumns, key, colData);
            
            if (newCol.getType().equals("object"))
            {
                if (colData.containsKey("$ref"))
                    colData = findDef((String) colData.get("$ref"),(JSONObject)schema.get("definitions"), schema);
                Table newTable = handleTable(key, user, colData, tables, tableColumns);

                parseSchemaProperties(schema, newTable, tables, columns, tableColumns, user,
                    (JSONObject) colData.get("properties"), defs);
            }
        }
    }
    
    public static Column handleColumn(Table parentTable, Map<String, List<Column>> columns, Map<String, List<Column>> tableColumns,
        String name, JSONObject colData)
    {
        Column newCol = new Column();
        newCol.setName(name);
        newCol.setType((String) colData.get("type"));
        newCol.setComment((String) colData.get("description"));
        newCol.setTable(parentTable);
        //columns.add(newCol);
        if (!columns.containsKey(name))        
            columns.put(name, new LinkedList<Column>());
        columns.get(name).add(newCol);
     
        tableColumns.get(parentTable.getName()).add(newCol);
        return newCol;
    }

    /**
     * Создает объект таблицы, заполняет его данными и добавляет в список таблиц
     * 
     * @param name    имя таблицы
     * @param user    пользователь-владелец
     * @param tabData данные о таблице
     * @param tables  список таблиц
     * @return        экземпляр таблицы с данными
     */
    public static Table handleTable(String name, User user, JSONObject tabData,
        /*List<Table>*/Map<String, Table> tables, Map<String, List<Column>> tableColumns)
    {
        Table newTable = new Table();
        newTable.setName(name);
        newTable.setType(Table.TYPE_TABLE);
        newTable.setOwner(user);
        newTable.setComment((String) tabData.get("description"));
        if(!tables.containsKey(name))
        {
            tables.put(name, newTable);
            tableColumns.put(name, new LinkedList<Column>());
            return newTable;
        }      
        //Mind this!
        else
            return tables.get(name);
    }

    //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    /**
     * Поиск значения ключа по заданной ссылке
     * 
     * @param path ссылка
     * @param obj  область поиска
     * @return     значение ключа по ссылке
     */
    public static JSONObject findDef(String path, JSONObject defs, JSONObject obj)
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
    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public JSONObject getSchema()
    {
        return schema;
    }

    public void setSchema(JSONObject schema)
    {
        this.schema = schema;
    }

    public Map<String, Table> getTables()
    {
        return tables;
    }

    public void setTables(Map<String, Table> tables)
    {
        this.tables = tables;
    }

    public Map<String, List<Column>> getColumns()
    {
        
        return columns;
    }
    
    public List<Column> getColumns(Table t)
    {
        return tableColumns.get(t.getName());
    }
    
    public List<Column> getColumns(String name)
    {
        return columns.get(name);
    }
    
    public Map<String, List<Column>> getTableColumns()
    {
        return tableColumns;
    }

    public void setTableColumns(Map<String, List<Column>> tableColumns)
    {
        this.tableColumns = tableColumns;
    }

    
}
