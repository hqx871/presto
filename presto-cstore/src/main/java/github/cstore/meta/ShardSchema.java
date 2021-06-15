package github.cstore.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ShardSchema
{
    private final List<ShardColumn> columns;
    private final int rowCount;
    private final int pageRowCount;

    @JsonCreator
    public ShardSchema(@JsonProperty("columns") List<ShardColumn> columns,
            @JsonProperty("rowCount") int rowCount,
            @JsonProperty("pageRowCount") int pageRowCount)
    {
        this.columns = columns;
        this.rowCount = rowCount;
        this.pageRowCount = pageRowCount;
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
    public int getPageRowCount()
    {
        return pageRowCount;
    }

    @Override
    public String toString()
    {
        return "ShardSchema{" +
                ", columns=" + columns +
                ", rowCount=" + rowCount +
                ", pageRowCount=" + pageRowCount +
                '}';
    }
}
