package org.apache.cstore.column;

import com.google.common.io.Files;
import org.apache.cstore.bitmap.Bitmap;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public final class ColumnBenchmarkTool
{
    private ColumnBenchmarkTool()
    {
    }

    public static LongColumnReader mapLongColumnReader(String path)
    {
        MappedByteBuffer buffer;
        try {
            buffer = Files.map(new File(path), FileChannel.MapMode.READ_ONLY);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new LongColumnReader(buffer.asLongBuffer());
    }

    public static Bitmap mapBitmapIndex(String path, int id)
    {
        MappedByteBuffer buffer;
        try {
            buffer = Files.map(new File(path), FileChannel.MapMode.READ_ONLY);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        BitmapColumnReader bitmapColumnReader = BitmapColumnReader.decode(buffer);
        return bitmapColumnReader.readObject(id);
    }
}
