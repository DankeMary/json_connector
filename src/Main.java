import java.nio.file.Path;
import java.nio.file.Paths;
import utils.SchemaUtils;

public class Main
{
    public static void main(String args[])
    {
        String s_path = "e:\\address.json";    
        SchemaUtils.parseJsonSchema(s_path);
    }
}
