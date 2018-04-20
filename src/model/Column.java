package model;
import java.sql.Types;

import lombok.Data;

@Data
public class Column
{
    public static final String TYPE_UNKNOWN = "UNKNOWN";
    public static final String TYPE_STRING = "VARCHAR2";
    public static final String TYPE_NUMBER = "NUMBER";
    public static final String TYPE_DATE = "DATE";
    public static final String TYPE_CLOB = "CLOB";
    public static final String TYPE_BLOB = "BLOB";

    private Table table;
    private String name;
    private String type;
    private String comment;
    private Table refTable;

    public Column()
    {
        super();
        setType(TYPE_UNKNOWN);
    }

    public static int convertToSqlType(String type)
    {
        if (TYPE_STRING.equals(type))
        {
            return Types.VARCHAR;
        }
        else if (TYPE_NUMBER.equals(type))
        {
            return Types.NUMERIC;
        }
        else if (TYPE_DATE.equals(type))
        {
            return Types.DATE;
        }
        else if (TYPE_CLOB.equals(type))
        {
            return Types.CLOB;
        }
        else if (TYPE_BLOB.equals(type))
        {
            return Types.BLOB;
        }

        return Types.VARCHAR;
    }
}
