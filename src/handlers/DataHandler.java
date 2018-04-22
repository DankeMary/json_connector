package handlers;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import model.Column;
import model.Table;
import model.TableRow;
import schema.JSONSchema;

public class DataHandler
{
    private JSONSchema schema;
    private JSONObject data;
    private Map<String, Map<TableRow, Integer>> tablesData;
    
    public Map<String, Map<TableRow, Integer>> getTablesData()
    {
        return tablesData;
    }

    public DataHandler()
    {
        schema = null;
        data = null;
        tablesData = new HashMap<>();
    }    
    
    public DataHandler(JSONSchema schema, JSONObject data)
    {
        this.schema = schema;
        this.data = data;
        tablesData = new HashMap<>();
    }
    
    /**
     * Запускает процесс взятия данных из JSON
     */
    public void handleData()
    {
        Table rootTable = schema.getTables().stream()
                .filter(table -> table.getName()
                    .equals(schema.getName().trim().toLowerCase()))
                .findFirst().get();
        getRow(rootTable, data);
    }    
    
    /**
     * Создает объект строки таблицы и заполняет его данными
     * @param table таблица
     * @param data  JSON с данными
     * @return      строка таблицы
     */
    private TableRow getRow(Table table, JSONObject data)
    {
        List<Column> columns = schema.getColumns(table);
        int colCount = columns.size();
        Object[] values = new Object[colCount];
        int colIndex = 0;
        
        for(Column c : columns)
        {
            Object value = data.get(c.getName());
            switch (c.getType())
            {
                case "array":
                    if (value != null)
                    {
                        List<Object> arrayData = new ArrayList<>();
                        
                        JSONArray array = (JSONArray) value;
                        if (array.size() > 0)
                        {
                            if (c.getRefTable() == null)
                            {
                                for (int i = 0; i < array.size(); i++)
                                {
                                    arrayData.add(array.get(i));
                                }
                            }
                            else
                            {
                                for (int i = 0; i < array.size(); i++)
                                {
                                    JSONObject arrValue = (JSONObject) array.get(i);
                                    if (arrValue == null)
                                    {
                                        arrayData.add(null);
                                    }
                                    else
                                    {
                                        TableRow parentRow = getRow(c.getRefTable(), arrValue);
                                        //arrayData.add(parentRow);
                                        arrayData.add(parentRow.getId());
                                    }
                                }
                            }
                            value = arrayData;
                        }
                    }
                    break;
                case "object":
                    if (value != null)
                    {
                        TableRow parentRow = getRow(c.getRefTable(),(JSONObject) value);
                        value = parentRow.getId();
                    }
                    break;                
                case "boolean": 
                    if(value.equals("false"))
                        value = 0;
                    else
                        value = 1; 
                    break;                 
            }
            values[colIndex] = value;
            colIndex++;
        }
        
        TableRow row = new TableRow();
        row.setTable(table);
        row.setValues(values);

        Map<TableRow, Integer> rows = tablesData
            .computeIfAbsent(table.getName(), k -> new HashMap<>());
        int maybeId = rows.size() + 1;
        Integer id = rows.putIfAbsent(row, maybeId);
        if (id != null)
        {
            maybeId = id;
        }
        row.setId(maybeId);
        
        return row;
    }
}
