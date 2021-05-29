package org.apache.cstore.io;

import org.apache.cstore.util.IOUtil;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

public class VectorWriterFactory
{
    private String dir;
    private String column;

    public VectorWriterFactory(String dir, String column)
    {
        this.dir = dir;
        this.column = column;
    }

    public File newFile(String name)
    {
        try {
            File file = new File(dir, name);
            if (file.exists()) {
                file.delete();
            }
            file.createNewFile();

            return file;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getName()
    {
        return column;
    }

    public String getDir()
    {
        return dir;
    }

    public DataOutputStream createFileStream(String file)
    {
        return IOUtil.openFileDataStream(newFile(file));
    }
}
