package github.cstore.io;

import github.cstore.util.IOUtil;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

public class VectorWriterFactory
{
    private final String dir;
    private final String name;
    private final String type;

    public VectorWriterFactory(String dir, String name, String type)
    {
        this.dir = dir;
        this.name = name;
        this.type = type;
    }

    public File newFile()
    {
        return newFile(type);
    }

    public File newFile(String type)
    {
        try {
            File file = new File(dir, name + "." + type);
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
        return name;
    }

    public String getDir()
    {
        return dir;
    }

    public DataOutputStream createFileStream(File file)
    {
        return IOUtil.openFileStream(file);
    }
}
