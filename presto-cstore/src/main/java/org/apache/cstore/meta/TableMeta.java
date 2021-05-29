package org.apache.cstore.meta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableMeta
{
    private String name;
    private ColumnMeta[] columns;
    private List<BitmapIndexMeta> bitmapIndexes;
    private int rowCnt;

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

    public ColumnMeta[] getColumns()
    {
        return columns;
    }

    public void setColumns(ColumnMeta[] columns)
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
}
