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
    
    private JSONObject schema;
    private String name;
    private List<Table> tables;
    //private static Map<String, String> realTableNames;     //Def - JSON
    private Map<String, List<Column>> columns;
    private Map<String, List<Column>> tableColumns;
    
    public JSONSchemaParser(JSONSchema objSchema)
    {
        schema = objSchema.getSchema();
        name = objSchema.getName();
        tables = objSchema.getTables();
        //realTableNames = objSchema.getRealTableNames();
        columns = objSchema.getColumns();
        //!!!Создать свой словарь, а потом присвоить его схеме
        tableColumns = objSchema.getTableColumns();
    }
    
    public void parse()
    {   
        User user = new User();
        user.setName(USER_NAME);

        Table rootTable = handleTable(name, user, schema);

        JSONObject properties = (JSONObject) schema.get("properties");

        parseProperties(rootTable, user, properties);
    }
    
    public void printTablesColumns()
    {
        for (Map.Entry<String, List<Column>> entry : tableColumns.entrySet()) {
            System.out.println("Table: " + entry.getKey());
            List<Column> cols = entry.getValue();
            for(Column c : cols)
                System.out.println("    " + c.getName());
        } 
    }
        
    public int getLevel(Table table)
    {
        List<Column> list = tableColumns.get(table.getName());
        int level = 0;
        for (Column column : list)
        {
            Table refTable = column.getRefTable();
            if (refTable != null)
            {
                level = Math.max(getLevel(refTable), level);
            }
        }

        return level + 1;
    }
    /**
     * Разбор пар в properties на столбцы и таблицы
     * 
     * @param parentTable таблица-владелец
     * @param user        пользователь-владелец
     * @param props       множество столбцов таблицы
     */
    private void parseProperties(Table parentTable, User user, JSONObject props)
    {
        for (Object keyObj : props.keySet())
        {
            String key = (String) keyObj;
            Object value = props.get(key);
            JSONObject colData = (JSONObject)value;
            Column newCol;

            StringBuilder realName = new StringBuilder();
            
            if (colData.containsKey("$ref"))
            {
                //String path = (String) colData.get("$ref");
                colData = findDef(realName, (String) colData.get("$ref"), schema);             
            }
            newCol = handleColumn(parentTable, key, colData);
            
            if (newCol.getType().equals("object") || newCol.getType().equals("array"))
            {
                if(newCol.getType().equals("array"))
                {
                    colData = (JSONObject)colData.get("items");
                }
                
                if (colData.containsKey("$ref"))
                {
                    //String path = (String) colData.get("$ref");                   
                    colData = findDef(realName, (String) colData.get("$ref"), schema);
                    /*if (tableColumns.containsKey(realName.toString()))
                        continue; */                                  
                }               
                
                if (colData.get("type").equals("object"))
                {         
                    Table refTable = tables.stream().filter(
                        table -> table.getName().equals(realName.toString()))
                        .findFirst().orElse(null);
                    if (refTable == null)
                    {
                        refTable = handleTable(realName.toString(), user,
                            colData);
                        parseProperties(refTable, user,
                            (JSONObject) colData.get("properties"));
                        //realTableNames.put(realName.toString(), key);
                    }
                    newCol.setRefTable(refTable);
                    /*if (tableColumns.containsKey(realName.toString()))
                        continue;*/                    
                }
            }            
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
    private Column handleColumn(Table parentTable, String name, JSONObject colData)
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
     * Создает объект таблицы, заполняет его данными и добавляет в список таблиц
     * 
     * @param name    имя таблицы
     * @param user    пользователь-владелец
     * @param tabData данные о таблице
     * @return        экземпляр таблицы с данными
     */
    private Table handleTable(String name, User user, JSONObject tabData)
    {
        name = name.trim().toLowerCase();
        Table newTable = new Table();
        newTable.setName(name);
        newTable.setType(Table.TYPE_TABLE);
        newTable.setOwner(user);
        newTable.setComment((String) tabData.get("description"));
        tables.add(0, newTable);
        tableColumns.put(name, new LinkedList<Column>());           
        
        return newTable;
    }

    /**
     * Поиск значения ключа по заданной ссылке
     * 
     * @param name имя объекта
     * @param path ссылка
     * @param obj  текущая область поиска
     * @return     значение ключа по ссылке
     */
    public JSONObject findDef(StringBuilder name, String path, JSONObject obj)
    {
        if (path == null || path.isEmpty())
            return null;
        if (path.indexOf("/") == -1)
        {   
            obj = (JSONObject) obj.get(path);
            if(obj.containsKey("$ref"))
                return findDef(name, (String) obj.get("$ref"), schema);        
            else
            {
                name.setLength(0);
                name.append(path);
                return obj;
            }
        }
        if (path.substring(0, 1).equals("#"))
            return findDef(name, path.substring(path.indexOf("/") + 1), obj);
        else
            return findDef(name, path.substring(path.indexOf("/") + 1), 
                (JSONObject) obj.get(path.substring(0, path.indexOf("/"))));
    }
}
