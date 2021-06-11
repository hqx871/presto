package github.cstore.meta;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ShardMeta
{
    private UUID uuid;
    private List<ShardColumn> columns;
    private int rowCnt;
    private int pageSize;

    public List<ShardColumn> getColumns()
    {
        return columns;
    }

    @JsonIgnore
    public Map<Long, ShardColumn> getColumnMap()
    {
        Map<Long, ShardColumn> map = new HashMap<>();
        for (ShardColumn columnMeta : columns) {
            map.put(columnMeta.getColumnId(), columnMeta);
        }
        return map;
    }

    public UUID getUuid()
    {
        return uuid;
    }

    public void setUuid(UUID uuid)
    {
        this.uuid = uuid;
    }

    public ShardColumn getColumn(String column)
    {
        return getColumnMap().get(column);
    }

    public void setColumns(List<ShardColumn> columns)
    {
        this.columns = columns;
    }

    public int getRowCnt()
    {
        return rowCnt;
    }

    public void setRowCnt(int rowCnt)
    {
        this.rowCnt = rowCnt;
    }

    public int getPageSize()
    {
        return pageSize;
    }

    public void setPageSize(int pageSize)
    {
        this.pageSize = pageSize;
    }

    @Override
    public String toString()
    {
        return "TableMeta{" +
                ", columns=" + columns +
                ", rowCnt=" + rowCnt +
                ", pageSize=" + pageSize +
                '}';
    }
}
