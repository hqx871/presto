package github.cstore.util;

import com.google.common.io.Files;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

@Deprecated
public final class IOUtil
{
    private IOUtil()
    {
    }

    public static void close(Closeable outputStream)
    {
        if (outputStream != null) {
            try {
                outputStream.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static FileOutputStream openFile(File file)
    {
        try {
            return new FileOutputStream(file);
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static DataOutputStream openFileStream(File file)
    {
        return new DataOutputStream(new BufferedOutputStream(openFile(file)));
        //return new DataOutputStream(openFile(file));
    }

    public static void flush(OutputStream output)
    {
        if (output != null) {
            try {
                output.flush();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static MappedByteBuffer mapFile(File file, MapMode mode)
    {
        try {
            return Files.map(file, mode);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
