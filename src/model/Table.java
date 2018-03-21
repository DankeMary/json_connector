
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

    public String getComment()
    {
        return comment;
    }

    public void setComment(String comment)
    {
        this.comment = comment;
    }

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public User getOwner()
    {
        return owner;
    }

    public void setOwner(User owner)
    {
        this.owner = owner;
    }
}
