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
}
