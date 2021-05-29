package org.apache.cstore.meta;

public class ColumnMeta
{
    private String version = "v1";
    private String name;
    private String typeName;
    private String fileName;

    private ColumnMeta bitmap;
    private ColumnMeta dict;

    private int cardinality;

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

    public ColumnMeta getBitmap()
    {
        return bitmap;
    }

    public void setBitmap(ColumnMeta bitmap)
    {
        this.bitmap = bitmap;
    }

    public ColumnMeta getDict()
    {
        return dict;
    }

    public void setDict(ColumnMeta dict)
    {
        this.dict = dict;
    }

    public int getCardinality()
    {
        return cardinality;
    }

    public void setCardinality(int cardinality)
    {
        this.cardinality = cardinality;
    }
}
