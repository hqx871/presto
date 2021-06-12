package github.cstore.meta;

import java.util.List;

public class ShardSchema
{
    private List<ShardColumn> columns;
    private int rowCount;
    private int pageByteSize;

    public List<ShardColumn> getColumns()
    {
        return columns;
    }

    public void setColumns(List<ShardColumn> columns)
    {
        this.columns = columns;
    }

    public int getRowCount()
    {
        return rowCount;
    }

    public void setRowCount(int rowCount)
    {
        this.rowCount = rowCount;
    }

    public int getPageByteSize()
    {
        return pageByteSize;
    }

    public void setPageByteSize(int pageByteSize)
    {
        this.pageByteSize = pageByteSize;
    }

    @Override
    public String toString()
    {
        return "ShardSchema{" +
                ", columns=" + columns +
                ", rowCount=" + rowCount +
                ", pageByteSize=" + pageByteSize +
                '}';
    }
}
