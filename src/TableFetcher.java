import java.util.List;

import org.json.simple.JSONObject;

import model.Column;
import model.Table;

public interface TableFetcher
{    
    List<Table> getTables(JSONSchema schema);
    List<Column> getColumns(JSONSchema schema);
    void getAllData(JSONSchema schema, JSONObject json);
    void getData(Column... columns);
}
