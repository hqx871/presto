package github.cstore.column;

import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ColumnFileLoader
{
    private final File directory;

    public ColumnFileLoader(File directory)
    {
        this.directory = directory;
    }

    public ByteBuffer open(String name)
    {
        File file = new File(directory, name);
        assert file.exists();
        try {
            return Files.map(file, FileChannel.MapMode.READ_ONLY);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
