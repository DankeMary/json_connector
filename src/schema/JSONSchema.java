package schema;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.json.simple.JSONObject;


import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import model.Column;
import model.Table;

@Data
public class JSONSchema
{    
    @Setter(AccessLevel.NONE)
    private String name;
    @Getter(AccessLevel.PACKAGE)
    @Setter(AccessLevel.NONE)
    private JSONObject schema;
    @Setter(AccessLevel.NONE) 
    private List<Table> tables;
    @Getter(AccessLevel.PACKAGE)
    @Setter(AccessLevel.NONE)
    private Map<String, List<Column>> columns;
    @Setter(AccessLevel.NONE)
    private Map<String, List<Column>> tableColumns;     

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
        tables = new LinkedList<Table>(); 
        columns = new HashMap<String, List<Column>>();
        tableColumns = new HashMap<String, List<Column>>();
        parse();
    }
   
    /**
     * Разбор схемы на таблицы и столбцы
     */
    public void parse()
    {   
        JSONSchemaParser parser = new JSONSchemaParser(this);        
        parser.parse();
    }
    
    public Table getTable(String name)
    {
        Optional<Table> table = tables
                .stream()
                .filter(t -> t.getName().equals(name))
                .findFirst();
        if(table.isPresent())
            return table.get();          
        else 
            return null;
    }
    
    /**
     * Получение списка столбцов таблицы
     * @param table таблица
     * @return      список столбцов 
     */
    public List<Column> getColumns(Table table)
    {
        return tableColumns.get(table.getName());
    }
    
    //getters & setters
    /*public String getName()
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

    public List<Table> getTables()
    {
        return tables;
    }
    
    public void setTables(List<Table> tables)
    {
        this.tables = tables;
    }

    public Map<String, List<Column>> getColumns()
    {        
        return columns;
    }
    public void setColumns(Map<String, List<Column>> columns)
    {
        this.columns = columns;
    }  
    
    /*public List<Column> getAllColumnsListed()
    {
        List<Column> res = new LinkedList<Column>();
        
        for (Map.Entry<String, List<Column>> entry : tableColumns.entrySet())            
            res.addAll(entry.getValue());
        return res;
    } 
    
    public List<Column> getColumns(Table t)
    {
        return tableColumns.get(t.getName());
    }
    
    public List<Column> getColumns(String name)
    {
        return columns.get(name);
    }*/
    
    /*public Map<String, List<Column>> getTableColumns()
    {
        return tableColumns;
    }*/
/*
    public void setTableColumns(Map<String, List<Column>> tableColumns)
    {
        this.tableColumns = tableColumns;
    }*/   
}
