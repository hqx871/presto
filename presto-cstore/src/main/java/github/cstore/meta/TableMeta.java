package github.cstore.meta;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableMeta
{
    private String name;
    private List<ColumnMeta> columns;
    private List<BitmapIndexMeta> bitmapIndexes;
    private int rowCnt;
    private int pageSize;

    public BitmapIndexMeta getBitmap(String column)
    {
        return getBitmapMap().get(column);
    }

    private Map<String, BitmapIndexMeta> getBitmapMap()
    {
        Map<String, BitmapIndexMeta> bitmapMap = new HashMap<>();
        for (BitmapIndexMeta indexMeta : bitmapIndexes) {
            bitmapMap.put(indexMeta.getName(), indexMeta);
        }
        return bitmapMap;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public List<ColumnMeta> getColumns()
    {
        return columns;
    }

    @JsonIgnore
    public Map<String, ColumnMeta> getColumnMap()
    {
        Map<String, ColumnMeta> map = new HashMap<>();
        for (ColumnMeta columnMeta : columns) {
            map.put(columnMeta.getName(), columnMeta);
        }
        return map;
    }

    public ColumnMeta getColumn(String column)
    {
        return getColumnMap().get(column);
    }

    public void setColumns(List<ColumnMeta> columns)
    {
        this.columns = columns;
    }

    public List<BitmapIndexMeta> getBitmapIndexes()
    {
        return bitmapIndexes;
    }

    public void setBitmapIndexes(List<BitmapIndexMeta> bitmapIndexes)
    {
        this.bitmapIndexes = bitmapIndexes;
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
                "name='" + name + '\'' +
                ", columns=" + columns +
                ", bitmapIndexes=" + bitmapIndexes +
                ", rowCnt=" + rowCnt +
                ", pageSize=" + pageSize +
                '}';
    }
}
