package org.apache.cstore.meta;

public class BitmapIndexMeta
{
    private String version = "v1";
    private String name;
    private String fileName;
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

    @Override
    public String toString()
    {
        return "BitmapIndexMeta{" +
                "version='" + version + '\'' +
                ", name='" + name + '\'' +
                ", fileName='" + fileName + '\'' +
                ", cardinality=" + cardinality +
                '}';
    }
}
