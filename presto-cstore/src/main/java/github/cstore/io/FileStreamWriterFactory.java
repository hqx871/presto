package github.cstore.io;

import java.io.File;
import java.io.IOException;

public class FileStreamWriterFactory
        implements StreamWriterFactory
{
    private final File directory;

    public FileStreamWriterFactory(File directory)
    {
        this.directory = directory;
    }

    @Override
    public StreamWriter createWriter(String name, boolean clean)
    {
        File file = new File(directory, name);
        if (file.exists() && !file.delete()) {
            throw new RuntimeException(String.format("delete %s fail", file.toString()));
        }

        try {
            assert file.createNewFile();
            return new FileStreamWriter(file, clean);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
