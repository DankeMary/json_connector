package database;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.sqlite.JDBC;

import com.google.common.base.Joiner;

import handlers.DatabaseHandler;
import model.Column;
import model.Table;
import model.TableRow;


public class SQLiteHandler implements DatabaseHandler
{
    private static final String COLUMN_ID = "\"$id\"";
    private final String DB_LOCATION = "";
    private String location = DB_LOCATION;
    private String connString;
    private Connection conn = null;

    public SQLiteHandler()
    {
        try
        {
            DriverManager.registerDriver(new JDBC());
            location = DB_LOCATION;
            connString = "";
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
            throw new RuntimeException();
        }
    }

    public SQLiteHandler(String dbName)
    {
        try
        {
            DriverManager.registerDriver(new JDBC());
            location = DB_LOCATION;
            connString = "jdbc:sqlite:" + location + dbName + ".db";
            conn = DriverManager.getConnection(connString);
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
            throw new RuntimeException();
        }
        finally
        {
            try
            {
                if (conn != null)
                {
                    conn.close();
                }
            }
            catch (SQLException e)
            {
                System.out.println(e.getMessage());
                throw new RuntimeException();
            }
        }
    }

    @Override
    public void createTable(Table table, List<Column> columns)
    {
        try
        {
            conn = DriverManager.getConnection(connString);
            String query = createTableQuery(table, columns);
            PreparedStatement pstmt = conn.prepareStatement(query);
            pstmt.executeUpdate();
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
            throw new RuntimeException();
        }
        finally
        {
            try
            {
                if (conn != null)
                {
                    conn.close();
                }
            }
            catch (SQLException e)
            {
                System.out.println(e.getMessage());
                throw new RuntimeException();
            }
        }
    }

    /**
     * Создает запрос для создания таблицы
     * 
     * @param table таблица
     * @param columns столбцы таблицы
     * @return строка с запросом
     */
    private String createTableQuery(Table table, List<Column> columns)
    {
        StringBuilder query = new StringBuilder(
            "CREATE TABLE IF NOT EXISTS \"");
        query = query.append(table.getName() + "\" (");
        query.append(COLUMN_ID + " INTEGER PRIMARY KEY ");
        for (Column c : columns)
        {
            Table refTable = c.getRefTable();
            query.append(", \"" + c.getName() + "\" ");
            switch (c.getType())
            {
                case "string":
                    query.append(" TEXT ");
                    break;
                case "integer":
                    query.append(" INTEGER ");
                    if (refTable != null)
                        query.append(" REFERENCES \"" + refTable.getName()
                            + "\"(" + COLUMN_ID + ") ");
                    break;
                case "array":
                    query.append(" TEXT ");
                    break;
                case "number":
                    query.append(" REAL ");
                    break;
                case "object":
                    query.append(" INTEGER REFERENCES \""
                        + c.getRefTable().getName() + "\"(" + COLUMN_ID + ") ");
                    break;
                case "boolean":
                    query.append(" NUMERIC ");
                    break;
            }
        }
        query.append(");");
        return query.toString();
    }

    /**
     * Проверяет столбцы на принадлежность таблице
     * 
     * @param table таблица
     * @param columns столбцы
     * @return true - все принадлежат, false - хотя бы 1 не принадлежит
     */
    private boolean checkColumns(Table table, Column... columns)
    {
        for (Column c : columns)
        {
            if (!table.getName().equals(c.getTable().getName()))
                return false;
        }
        return true;
    }

    @Override
    public List<String> getData(Table table, Column... columns)
    {
        if (!checkColumns(table, columns))
        {
            System.out.println(
                "1 или более столбцовне принадлежат указанной таблице!");
            throw new RuntimeException();
        }
        else
            try
            {
                StringBuilder query = new StringBuilder("SELECT ");
                for (Column c : columns)
                {
                    query.append("\"" + c.getName() + "\",");
                }
                query.setLength(query.length() - 1);
                query.append(" FROM \"" + table.getName() + "\";");

                conn = DriverManager.getConnection(connString);
                PreparedStatement pstmt = conn
                    .prepareStatement(query.toString());

                ResultSet res = pstmt.executeQuery();
                List<String> resList = new LinkedList<String>();

                while (res.next())
                {
                    StringBuilder str = new StringBuilder();
                    for (Column c : columns)
                    {
                        switch (c.getType())
                        {
                            case "array":
                                Object array = res.getObject(c.getName());
                                if (array == null)
                                    str.append("\"null\"  ");
                                else
                                    str.append("\"[array]\"  ");
                                break;
                            case "string":
                                String valStr = res.getString(c.getName());
                                if (res.wasNull())
                                    str.append("\"null\"  ");
                                else
                                    str.append("\"" + valStr + "\"  ");
                                break;
                            case "integer":
                            case "object":
                                long valLong = res.getLong(c.getName());
                                if (res.wasNull())
                                    str.append("\"null\"  ");
                                else
                                    str.append("\"" + valLong + "\"  ");
                                break;
                            case "number":
                                double valDouble = res.getDouble(c.getName());
                                if (res.wasNull())
                                    str.append("\"null\"  ");
                                else
                                    str.append("\"" + valDouble + "\"  ");
                                break;
                            case "boolean":
                                boolean valBoolean = res
                                    .getBoolean(c.getName());
                                if (res.wasNull())
                                    str.append("\"null\"  ");
                                else
                                    str.append("\"" + valBoolean + "\"  ");
                                break;
                        }
                    }
                    resList.add(str.toString());
                }
                return resList;
            }
            catch (SQLException e)
            {
                System.out.println(e.getMessage());
                throw new RuntimeException();
            }
            finally
            {
                try
                {
                    if (conn != null)
                    {
                        conn.close();
                    }
                }
                catch (SQLException e)
                {
                    System.out.println(e.getMessage());
                    throw new RuntimeException();
                }
            }
    }

    @Override
    public void uploadData(Map<String, List<Column>> tableColumns, Table table,
        Map<TableRow, Integer> rows)
    {
        try
        {
            conn = DriverManager.getConnection(connString);
            conn.setAutoCommit(false);

            String tableName = table.getName();
            List<Column> columns = tableColumns.get(tableName);
            String query = createInsertQuery(tableName, columns);

            if (rows != null)
                try (PreparedStatement stmt = conn.prepareStatement(query))
                {
                    for (TableRow row : rows.keySet())
                    {
                        stmt.setInt(1, row.getId());
                        int colIndex = 2;
                        Object[] values = row.getValues();
                        for (Column col : columns)
                        {
                            Object value = values[colIndex - 2];
                            if (value == null)
                            {
                                stmt.setNull(colIndex, Types.INTEGER);
                            }
                            else if (col.getType().equals("array"))
                            {
                                String crossTableName, crossTableQuery;
                                if (col.getRefTable() != null)
                                {
                                    crossTableName = tableName + "_"
                                        + col.getRefTable().getName();
                                    crossTableQuery = createInsertQuery(
                                        crossTableName,
                                        tableColumns.get(crossTableName));
                                }
                                else
                                {
                                    crossTableName = tableName + "_"
                                        + col.getName();
                                    crossTableQuery = createInsertQuery(
                                        crossTableName,
                                        tableColumns.get(crossTableName));
                                }

                                PreparedStatement stmt1 = conn
                                    .prepareStatement(crossTableQuery);
                                for (Object o : (List<Object>) value)
                                {
                                    stmt1.setObject(2, row.getId());
                                    stmt1.setObject(3, o);
                                    stmt1.addBatch();
                                }
                                stmt1.executeBatch();

                                stmt.setObject(colIndex, "[array]");
                            }
                            else
                            {
                                stmt.setObject(colIndex, value);
                            }
                            colIndex++;
                        }
                        stmt.addBatch();
                    }

                    stmt.executeBatch();
                    conn.commit();
                }
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
            throw new RuntimeException();
        }
        finally
        {
            try
            {
                if (conn != null)
                {
                    conn.close();
                }
            }
            catch (SQLException e)
            {
                System.out.println(e.getMessage());
                throw new RuntimeException();
            }
        }
    }

    /**
     * Создает строку запроса для вставки данных в таблицу
     * 
     * @param table таблица
     * @param columns столбцы таблицы
     * @return строка запроса
     */
    private String createInsertQuery(String tableName, List<Column> columns)
    {
        StringBuilder query = new StringBuilder(
            "INSERT INTO \"" + tableName + "\"(" + COLUMN_ID + ", ");

        Joiner.on(',').appendTo(query, columns.stream()
            .map(column -> "\"" + column.getName() + "\"").iterator());
        query.append(") VALUES(?,");

        Joiner.on(',').appendTo(query,
            columns.stream().map(column -> "?").iterator());
        query.append(");");
        return query.toString();
    }

    @Override
    public List<String> getData(Table table, List<Column> columns)
    {
        try
        {
            String query = "SELECT * FROM \"" + table.getName() + "\";";
            conn = DriverManager.getConnection(connString);
            PreparedStatement pstmt = conn.prepareStatement(query);
            ResultSet res = pstmt.executeQuery();
            List<String> resList = new LinkedList<String>();

            while (res.next())
            {
                StringBuilder str = new StringBuilder(
                    "\"" + String.valueOf(res.getLong("$id")) + "\"");
                for (Column c : columns)
                {
                    switch (c.getType())
                    {
                        case "string":
                        case "array":
                            String valStr = res.getString(c.getName());
                            if (res.wasNull())
                                str.append("  \"null\"");
                            else
                                str.append("  \"" + valStr + "\"");
                            break;
                        case "integer":
                        case "object":
                            long valLong = res.getLong(c.getName());
                            if (res.wasNull())
                                str.append("  \"null\"");
                            else
                                str.append("  \"" + valLong + "\"");
                            break;
                        case "number":
                            double valDouble = res.getDouble(c.getName());
                            if (res.wasNull())
                                str.append("  \"null\"");
                            else
                                str.append("  \"" + valDouble + "\"");
                            break;
                        case "boolean":
                            long valBool = res.getLong(c.getName());
                            if (res.wasNull())
                                str.append("  \"null\"");
                            else if (valBool == 0)
                                str.append("  \"false\"");
                            else
                                str.append("  \"true\"");
                            break;

                    }
                }
                resList.add(str.toString());
            }
            return resList;
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
            throw new RuntimeException();
        }
        finally
        {
            try
            {
                if (conn != null)
                {
                    conn.close();
                }
            }
            catch (SQLException e)
            {
                System.out.println(e.getMessage());
                throw new RuntimeException();
            }
        }
    }
}
