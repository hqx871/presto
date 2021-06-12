package github.cstore.meta;

import java.util.List;

public class ShardSchema
{
    private List<ShardColumn> columns;
    private int rowCnt;
    private int pageSize;

    public List<ShardColumn> getColumns()
    {
        return columns;
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
        return "ShardSchema{" +
                ", columns=" + columns +
                ", rowCnt=" + rowCnt +
                ", pageSize=" + pageSize +
                '}';
    }
}
