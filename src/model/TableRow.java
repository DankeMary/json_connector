package model;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(of = {"values"})
public class TableRow
{
    /**
     * id строки в таблице
     */
    private int id;
    /**
     * таблица-владелец
     */
    private Table table;
    /**
     * значения из строки
     */
    private Object[] values;
}
