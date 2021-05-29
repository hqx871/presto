package org.apache.cstore.meta;

import java.util.List;

public class DbMeta
{
    private int tableCount;
    private List<TableMeta> tables;

    public DbMeta(int tableCount, List<TableMeta> tables)
    {
        this.tableCount = tableCount;
        this.tables = tables;
    }

    public void addTable(TableMeta table)
    {
        tables.add(table);
    }

    public int getTableCount()
    {
        return tableCount;
    }

    public void setTableCount(int tableCount)
    {
        this.tableCount = tableCount;
    }

    public List<TableMeta> getTables()
    {
        return tables;
    }

    public void setTables(List<TableMeta> tables)
    {
        this.tables = tables;
    }
}
