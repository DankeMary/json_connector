package model;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(of = {"values"})
public class TableRow
{
    private int id;
    private Table table;
    private Object[] values;
}
