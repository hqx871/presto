package org.apache.cstore.column;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cstore.CStoreColumnHandle;
import com.facebook.presto.cstore.CStoreSplit;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;

public class CStoreColumnReaderFactory
{
    public CStoreColumnReader open(CStoreSplit split, CStoreColumnHandle columnHandle)
    {
        Type type = columnHandle.getColumnType();
        String path = split.getPath();
        switch (type.getClass().getSimpleName()) {
            case "IntegerType":
                return openIntReader(path, columnHandle.getColumnName(), (IntegerType) type);
            case "BigintType":
                return openLongReader(path, columnHandle.getColumnName(), (BigintType) type);
            case "DoubleType":
                return openDoubleReader(path, columnHandle.getColumnName(), (DoubleType) type);
            case "CharType":
                return openStringReader(path, columnHandle.getColumnName(), (CharType) type);
            default:
        }
        throw new UnsupportedOperationException();
    }

    private CStoreColumnReader openStringReader(String path, String name, CharType type)
    {
        ByteBuffer mapped = openFile(path, name);
        int dataSize = mapped.getInt(mapped.limit() - 4);
        mapped.position(mapped.limit() - 4 - dataSize);
        ByteBuffer data = mapped.slice();
        data.limit(dataSize);

        int bitmapSize = mapped.getInt(mapped.limit() - 8 - dataSize);
        mapped.position(mapped.limit() - 8 - dataSize - bitmapSize);
        ByteBuffer bitmap = mapped.slice();
        bitmap.limit(bitmapSize);

        int dictSize = mapped.getInt(mapped.limit() - 12 - dataSize - bitmapSize);
        mapped.position(mapped.limit() - 12 - dataSize - bitmapSize - dictSize);
        ByteBuffer dict = mapped.slice();
        dict.limit(dictSize);
        BitmapColumnReader bitmapVector = BitmapColumnReader.decode(bitmap);
        return StringEncodedColumnReader.decode(data, dict);
    }

    private CStoreColumnReader openIntReader(String path, String name, IntegerType type)
    {
        ByteBuffer buffer = openFile(path, name);
        IntBuffer intBuffer = buffer.asIntBuffer();
        return new IntColumnReader(intBuffer);
    }

    private CStoreColumnReader openLongReader(String path, String name, BigintType type)
    {
        return new LongColumnReader(openFile(path, name).asLongBuffer());
    }

    private CStoreColumnReader openDoubleReader(String path, String name, DoubleType type)
    {
        return new DoubleColumnarReader(openFile(path, name).asDoubleBuffer());
    }

    private static ByteBuffer openFile(String dir, String name)
    {
        File file = new File(dir, name + ".bin");
        try {
            return Files.map(file, FileChannel.MapMode.READ_ONLY);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
