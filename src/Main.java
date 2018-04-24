import test.TestClass;


public class Main
{
    public static void main(String args[])
    {
        String schema_path = "some_json_path";
        String data_path = "some_schema_path";

        TestClass.runTest(schema_path, data_path);
    }
}
