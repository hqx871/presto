package org.apache.cstore.meta;

public class TableMeta
{
    private String name;
    private ColumnMeta[] column;
    private int rowCnt;

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public ColumnMeta[] getColumn()
    {
        return column;
    }

    public void setColumn(ColumnMeta[] column)
    {
        this.column = column;
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
