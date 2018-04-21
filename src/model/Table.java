package model;

import lombok.Data;

@Data
public class Table
{
    public static final String TYPE_TABLE = "TABLE";
    public static final String TYPE_VIEW = "VIEW";

    private User owner;
    private String name;
    private String type;
    private String comment;

    public Table()
    {
        super();
        setType(TYPE_TABLE);
    }    
}
