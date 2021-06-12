package github.cstore.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ShardSchema
{
    private final List<ShardColumn> columns;
    private final int rowCount;
    private final int pageByteSize;

    @JsonCreator
    public ShardSchema(@JsonProperty("columns") List<ShardColumn> columns,
            @JsonProperty("rowCount") int rowCount,
            @JsonProperty("pageByteSize") int pageByteSize)
    {
        this.columns = columns;
        this.rowCount = rowCount;
        this.pageByteSize = pageByteSize;
    }

    @JsonProperty
    public List<ShardColumn> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public int getRowCount()
    {
        return rowCount;
    }

    @JsonProperty
    public int getPageByteSize()
    {
        return pageByteSize;
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
