package github.cstore.io;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

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
        //File file = new File(directory, name);
        File file = new File(directory, UUID.randomUUID().toString() + ".bin");
        if (file.exists() && !file.delete()) {
            throw new RuntimeException(String.format("delete %s fail", file.toString()));
        }

        try {
            file.createNewFile();
            return new FileStreamWriter(file, clean);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
