package github.cstore.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ShardColumn
{
    private final String version;
    private final long columnId;
    private final String typeName;
    private final String fileName;
    private final int cardinality;
    private final boolean dictionaryEncode;
    private final String compressType;
    private final int byteSize;
    private final boolean hasBitmap;

    @JsonCreator
    public ShardColumn(@JsonProperty("version") String version,
            @JsonProperty("columnId") long columnId,
            @JsonProperty("typeName") String typeName,
            @JsonProperty("fileName") String fileName,
            @JsonProperty("cardinality") int cardinality,
            @JsonProperty("dictionaryEncode") boolean dictionaryEncode,
            @JsonProperty("compressType") String compressType,
            @JsonProperty("byteSize") int byteSize,
            @JsonProperty("hasBitmap") boolean hasBitmap)
    {
        this.version = version;
        this.columnId = columnId;
        this.typeName = typeName;
        this.fileName = fileName;
        this.cardinality = cardinality;
        this.dictionaryEncode = dictionaryEncode;
        this.compressType = compressType;
        this.byteSize = byteSize;
        this.hasBitmap = hasBitmap;
    }

    @JsonProperty
    public int getByteSize()
    {
        return byteSize;
    }

    @JsonProperty
    public boolean isHasBitmap()
    {
        return hasBitmap;
    }

    @JsonProperty
    public long getColumnId()
    {
        return columnId;
    }

    @JsonProperty
    public boolean isDictionaryEncode()
    {
        return dictionaryEncode;
    }

    @JsonProperty
    public String getVersion()
    {
        return version;
    }

    @JsonProperty
    public String getTypeName()
    {
        return typeName;
    }

    @JsonProperty
    public String getFileName()
    {
        return fileName;
    }

    @JsonProperty
    public int getCardinality()
    {
        return cardinality;
    }

    @JsonProperty
    public String getCompressType()
    {
        return compressType;
    }

    @Override
    public String toString()
    {
        return "ShardColumn{" +
                "version='" + version + '\'' +
                ", typeName='" + typeName + '\'' +
                ", fileName='" + fileName + '\'' +
                ", cardinality=" + cardinality +
                ", dictionaryEncode=" + dictionaryEncode +
                ", compressType='" + compressType + '\'' +
                '}';
    }
}
