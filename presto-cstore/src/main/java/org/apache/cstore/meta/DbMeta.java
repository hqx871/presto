package org.apache.cstore.meta;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DbMeta
{
    private String name;
    private List<TableMeta> tables;

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public List<TableMeta> getTables()
    {
        return tables;
    }

    public void setTables(List<TableMeta> tables)
    {
        this.tables = tables;
    }

    public Map<String, TableMeta> getTableMap()
    {
        return tables.stream().collect(Collectors.toMap(TableMeta::getName, Function.identity()));
    }
}
