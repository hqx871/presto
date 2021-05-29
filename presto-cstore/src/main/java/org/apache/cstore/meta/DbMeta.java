package org.apache.cstore.meta;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DbMeta
{
    private String name;
    private List<TableMeta> tableList;

    public DbMeta(String name, List<TableMeta> tables)
    {
        this.name = name;
        this.tableList = tables;
    }

    public void addTable(TableMeta table)
    {
        tableList.add(table);
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public List<TableMeta> getTableList()
    {
        return tableList;
    }

    public void setTableList(List<TableMeta> tableList)
    {
        this.tableList = tableList;
    }

    public Map<String, TableMeta> getTableMap()
    {
        return tableList.stream().collect(Collectors.toMap(TableMeta::getName, Function.identity()));
    }
}
