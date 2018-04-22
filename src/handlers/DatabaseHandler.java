package handlers;

import java.util.List;
import java.util.Map;

import model.Column;
import model.Table;
import model.TableRow;

public interface DatabaseHandler
{
    /**Создает таблицу в базе данных
     * @param table    таблица
     * @param columns столбцы таблицы
     */
    void createTable(Table table, List<Column> columns);
    /**
     * Возвращает значения всех столбцов из таблицы
     * @param table       таблица
     * @param columns список всех столбцов
     */
    List<String> getData(Table table, Column... columns);
    /**
     * Возвращает значения указанных столбцов из таблицы
     * @param table       таблица
     * @param columns столбцы
     */
    List<String> getData(Table table, List<Column> columns);
    /**
     * Загружает данные в таблицу
     * @param tableColumns словарь таблиц и столбцов
     * @param table        целевая таблица
     * @param rows         строки для вставки
     */
    void uploadData(Map<String, List<Column>> tableColumns, Table table, Map<TableRow, Integer> rows);
}
