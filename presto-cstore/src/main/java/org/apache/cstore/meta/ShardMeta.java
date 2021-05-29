package org.apache.cstore.meta;

public class ShardMeta
{
    private String[] name;
    private ColumnMeta[] columns;
    private BitmapIndexMeta[] bitmapIndexes;
    private int rowCnt;

    public String[] getName()
    {
        return name;
    }

    public void setName(String[] name)
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

    public BitmapIndexMeta[] getBitmapIndexes()
    {
        return bitmapIndexes;
    }

    public void setBitmapIndexes(BitmapIndexMeta[] bitmapIndexes)
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
