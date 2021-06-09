package org.apache.cstore.meta;

public class ColumnMeta
{
    private String version = "v1";
    private String name;
    private String typeName;
    private String fileName;
    private int cardinality;
    private boolean dictionaryEncode;
    private String compressType;

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

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
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
        return "ColumnMeta{" +
                "version='" + version + '\'' +
                ", name='" + name + '\'' +
                ", typeName='" + typeName + '\'' +
                ", fileName='" + fileName + '\'' +
                ", cardinality=" + cardinality +
                ", dictionaryEncode=" + dictionaryEncode +
                ", compressType='" + compressType + '\'' +
                '}';
    }
}
