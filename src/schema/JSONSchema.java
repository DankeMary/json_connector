package schema;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.json.simple.JSONObject;


import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import model.Column;
import model.Table;
import model.User;

@Data
public class JSONSchema
{    
    private String name;
    private JSONObject schema;
    //@Getter(AccessLevel.NONE)
    //@Setter(AccessLevel.NONE) 
    private List<Table> tables;
    //private Map<String, String> realTableNames;
    //@Getter(AccessLevel.NONE)
    //@Setter(AccessLevel.NONE)
    private Map<String, List<Column>> columns;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Map<String, List<Column>> tableColumns;     

    public JSONSchema()
    {
        name = "";
        schema = null;
        tables = null;
        //realTableNames = null;
        columns = null;
        tableColumns = null;
    }
    
    public JSONSchema(String name, JSONObject objSchema)
    {
        this.name = name; 
        schema = objSchema;
        tables = new LinkedList<Table>(); 
        //realTableNames = new HashMap<String, String>();
        columns = new HashMap<String, List<Column>>();
        tableColumns = new HashMap<String, List<Column>>();
        parse();
    }
    
   /* public String getJSONName(String defName)
    {
        return realTableNames.get(defName);
    }
    
    public String getDefName(String jsonName)
    {
        for (Map.Entry<String, String> entry : realTableNames.entrySet()) {
            if(jsonName.equals(entry.getValue()))
                return entry.getKey();
        }
        return null;
    }
    
    public Map<String, String> getRealTableNames()
    {
        return realTableNames;
    }

    public void setRealTableNames(Map<String, String> realTableNames)
    {
        this.realTableNames = realTableNames;
    }*/

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
    
    public List<Column> getColumns(Table t)
    {
        return tableColumns.get(t.getName());
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
    
    public Map<String, List<Column>> getTableColumns()
    {
        return tableColumns;
    }
/*
    public void setTableColumns(Map<String, List<Column>> tableColumns)
    {
        this.tableColumns = tableColumns;
    }*/   
}
