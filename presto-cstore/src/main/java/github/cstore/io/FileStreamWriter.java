package github.cstore.io;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class FileStreamWriter
        extends DataStreamWriter
{
    private File file;
    private final boolean delete;
    private int offset;

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
    public ByteBuffer toByteBuffer()
    {
        try {
            flush();
            FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
            return fileChannel.map(FileChannel.MapMode.READ_ONLY, offset, fileChannel.size() - offset);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void reset()
    {
        offset = (int) file.length();
    }
}
