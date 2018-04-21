package schema;
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
    private Map<String, List<Column>> columns;
    private Map<String, List<Column>> tableColumns;
    
    public JSONSchemaParser(JSONSchema objSchema)
    {
        schema = objSchema.getSchema();
        name = objSchema.getName();
        tables = objSchema.getTables();
        columns = objSchema.getColumns();
        tableColumns = objSchema.getTableColumns();
    }
    
    /**
     * Запускает процесс разбора схемы
     */
    public void parse()
    {   
        User user = new User();
        user.setName(USER_NAME);

        Table rootTable = handleTable(name, user, schema);

        JSONObject properties = (JSONObject) schema.get("properties");

        parseProperties(rootTable, user, properties);
    }
    
    /**
     * Вычисление уровня таблицы
     * @param table таблица
     * @return      уровень
     */
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
                    colData = findDef(realName, (String) colData.get("$ref"), schema); 
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
                        if(newCol.getType().equals("array"))
                        {
                            handleCrossTable(user, parentTable, refTable);
                        }
                    }
                    newCol.setRefTable(refTable);                                      
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
     * @param tableName    имя таблицы
     * @param user    пользователь-владелец
     * @param tableData данные о таблице
     * @return        экземпляр таблицы с данными
     */
    private Table handleTable(String tableName, User user, JSONObject tableData)
    {
        tableName = tableName.trim().toLowerCase();
        Table newTable = new Table();
        newTable.setName(tableName);
        newTable.setType(Table.TYPE_TABLE);
        newTable.setOwner(user);
        newTable.setComment((String) tableData.get("description"));
        tables.add(newTable);
        tableColumns.put(tableName, new LinkedList<Column>());           
        
        return newTable;
    }
    
    /**
     * Создает промежуточную таблицу для хранения массивов
     * @param user       пользователь-владелец
     * @param leftTable  таблица-владелец
     * @param rightTable таблица-потомок
     */
    private void handleCrossTable(User user, Table leftTable, Table rightTable)
    {
        String tableName = leftTable.getName() + "_" + rightTable.getName();
        Table table = new Table();
        table.setName(tableName);
        table.setType(Table.TYPE_TABLE);
        table.setOwner(user);
        tables.add(table);
        tableColumns.put(tableName, new LinkedList<Column>()); 
        
        Column leftColumn = new Column();
        leftColumn.setName(leftTable.getName() + "_id");        
        leftColumn.setType("object");
        leftColumn.setRefTable(leftTable);
        tableColumns.get(tableName).add(leftColumn);
        if (!columns.containsKey(leftColumn.getName()))        
            columns.put(leftColumn.getName(), new LinkedList<Column>());
        columns.get(leftColumn.getName()).add(leftColumn);
        
        Column rightColumn = new Column();        
        rightColumn.setName(rightTable.getName() + "_id");        
        rightColumn.setType("object");        
        rightColumn.setRefTable(rightTable);       
        tableColumns.get(tableName).add(rightColumn);        
        if (!columns.containsKey(rightColumn.getName()))        
            columns.put(rightColumn.getName(), new LinkedList<Column>());
        columns.get(rightColumn.getName()).add(leftColumn);
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
    
        String[] splitPath = path.split("/");
        for(String item : splitPath)
        {
            if(item.equals("#"))
                continue;
            obj = (JSONObject) obj.get(item);
        }
        if(obj.containsKey("$ref"))
            return findDef(name, (String) obj.get("$ref"), schema);        
        else
        {
            name.setLength(0);
            name.append(path.substring(path.lastIndexOf("/") + 1));
            return obj;
        }
    }
}
