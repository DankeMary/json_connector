package database;

import java.util.List;
import java.util.Map;

import model.Column;
import model.Table;
import model.TableRow;

public interface DatabaseHandler
{
    /**Создает таблицу в базе данных
     * @param t    таблица
     * @param cols столбцы таблицы
     */
    void createTable(Table t, List<Column> cols);
    /**
     * Возвращает значения всех столбцов из таблицы
     * @param t       таблица
     * @param columns список всех столбцов
     */
    List<String> getData(Table t, Column... columns);
    /**
     * Возвращает значения указанных столбцов из таблицы
     * @param t       таблица
     * @param columns столбцы
     */
    List<String> getData(Table t, List<Column> columns);
    /**
     * Загружает данные в таблицу
     * @param tableColumns словарь таблиц и столбцов
     * @param table        целевая таблица
     * @param rows         строки для вставки
     */
    void uploadData(Map<String, List<Column>> tableColumns, Table table, Map<TableRow, Integer> rows);
}
