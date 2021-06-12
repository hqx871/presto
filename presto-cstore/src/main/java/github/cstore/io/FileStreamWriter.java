package github.cstore.io;

import com.google.common.io.Files;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileStreamWriter
        extends OutputStreamWriter
{
    private File file;
    private final boolean delete;

    public FileStreamWriter(File file, boolean delete)
    {
        super(openFileStream(file));
        this.file = file;
        this.delete = delete;
    }

    private static DataOutputStream openFileStream(File file)
    {
        try {
            return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete()
    {
        if (delete && file != null && !file.delete()) {
            throw new RuntimeException(String.format("delete %s fail", file.toString()));
        }
        file = null;
    }

    @Override
    public ByteBuffer map()
    {
        try {
            return Files.map(file, FileChannel.MapMode.READ_ONLY);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
