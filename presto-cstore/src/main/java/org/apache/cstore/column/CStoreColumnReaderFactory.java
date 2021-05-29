package org.apache.cstore.column;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
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
    public CStoreColumnReader open(String path, String column, Type type)
    {
        switch (type.getClass().getSimpleName()) {
            case "IntegerType":
                return openIntReader(path, column, (IntegerType) type);
            case "BigintType":
                return openLongReader(path, column, (BigintType) type);
            case "DoubleType":
                return openDoubleReader(path, column, (DoubleType) type);
            case "VarcharType":
                return openStringReader(path, column, (VarcharType) type);
            default:
        }
        throw new UnsupportedOperationException();
    }

    public CStoreColumnReader open(CStoreSplit split, CStoreColumnHandle columnHandle)
    {
        Type type = columnHandle.getColumnType();
        String path = split.getPath();
        return open(path, columnHandle.getColumnName(), type);
    }

    public CStoreColumnReader openStringReader(String path, String name, VarcharType type)
    {
        ByteBuffer mapped = openFile(path, name, ".bin");
        int dataSize = mapped.getInt(mapped.limit() - Integer.BYTES);
        mapped.position(mapped.limit() - Integer.BYTES - dataSize);
        ByteBuffer data = mapped.slice();
        data.limit(dataSize);

        int dictSize = mapped.getInt(mapped.limit() - 2 * Integer.BYTES - dataSize);
        mapped.position(mapped.limit() - 2 * Integer.BYTES - dataSize - dictSize);
        ByteBuffer dict = mapped.slice();
        dict.limit(dictSize);

        return StringEncodedColumnReader.decode(type, data, dict);
    }

    private CStoreColumnReader openIntReader(String path, String name, IntegerType type)
    {
        ByteBuffer buffer = openFile(path, name, ".bin");
        IntBuffer intBuffer = buffer.asIntBuffer();
        return new IntColumnReader(intBuffer);
    }

    private CStoreColumnReader openLongReader(String path, String name, BigintType type)
    {
        return new LongColumnReader(openFile(path, name, ".bin").asLongBuffer());
    }

    private CStoreColumnReader openDoubleReader(String path, String name, DoubleType type)
    {
        return new DoubleColumnarReader(openFile(path, name, ".bin").asDoubleBuffer());
    }

    private static ByteBuffer openFile(String dir, String name, String suffix)
    {
        File file = new File(dir, name + suffix);
        try {
            return Files.map(file, FileChannel.MapMode.READ_ONLY);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
