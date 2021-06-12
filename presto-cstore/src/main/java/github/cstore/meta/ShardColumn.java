package github.cstore.meta;

public class ShardColumn
{
    private String version = "v1";
    private long columnId;
    private String typeName;
    private String fileName;
    private int cardinality;
    private boolean dictionaryEncode;
    private String compressType;
    private int byteSize;
    private boolean hasBitmap;

    public int getByteSize()
    {
        return byteSize;
    }

    public void setByteSize(int byteSize)
    {
        this.byteSize = byteSize;
    }

    public boolean isHasBitmap()
    {
        return hasBitmap;
    }

    public void setHasBitmap(boolean hasBitmap)
    {
        this.hasBitmap = hasBitmap;
    }

    public long getColumnId()
    {
        return columnId;
    }

    public void setColumnId(long columnId)
    {
        this.columnId = columnId;
    }

    public boolean isDictionaryEncode()
    {
        return dictionaryEncode;
    }

    public void setDictionaryEncode(boolean dictionaryEncode)
    {
        this.dictionaryEncode = dictionaryEncode;
    }

    public String getVersion()
    {
        return version;
    }

    public void setVersion(String version)
    {
        this.version = version;
    }

    public String getTypeName()
    {
        return typeName;
    }

    public void setTypeName(String typeName)
    {
        this.typeName = typeName;
    }

    public String getFileName()
    {
        return fileName;
    }

    public void setFileName(String fileName)
    {
        this.fileName = fileName;
    }

    public int getCardinality()
    {
        return cardinality;
    }

    public void setCardinality(int cardinality)
    {
        this.cardinality = cardinality;
    }

    public String getCompressType()
    {
        return compressType;
    }

    public void setCompressType(String compressType)
    {
        this.compressType = compressType;
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
