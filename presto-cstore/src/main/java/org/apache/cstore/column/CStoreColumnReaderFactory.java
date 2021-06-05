package org.apache.cstore.column;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.cstore.CStoreColumnHandle;
import com.facebook.presto.cstore.CStoreSplit;
import com.google.common.io.Files;
import io.airlift.compress.Decompressor;
import org.apache.cstore.dictionary.ImmutableTrieTree;
import org.apache.cstore.dictionary.SstDictionary;
import org.apache.cstore.dictionary.StringDictionary;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;

public class CStoreColumnReaderFactory
{
    public CStoreColumnReader open(int rowCount, int pageSize, Decompressor decompressor, String path, String column, Type type)
    {
        switch (type.getClass().getSimpleName()) {
            case "IntegerType":
                return openIntZipReader(path, column, (IntegerType) type, rowCount, pageSize, decompressor);
            case "BigintType":
                return openLongZipReader(path, column, (BigintType) type, rowCount, pageSize, decompressor);
            case "DoubleType":
                return openDoubleZipReader(path, column, (DoubleType) type, rowCount, pageSize, decompressor);
            case "VarcharType":
                return openStringReader(rowCount, pageSize, decompressor, path, column, (VarcharType) type);
            default:
        }
        throw new UnsupportedOperationException();
    }

    public CStoreColumnReader open(Decompressor decompressor, CStoreSplit split, CStoreColumnHandle columnHandle)
    {
        Type type = columnHandle.getColumnType();
        String path = split.getPath();
        return open(split.getRowCount(), 64 << 10, decompressor, path, columnHandle.getColumnName(), type);
    }

    @Deprecated
    public StringEncodedColumnReader openStringReader(int rowCount, int pageSize, Decompressor decompressor, String path, String name, VarcharType type)
    {
        return openStringReader(rowCount, pageSize, decompressor, path, name, false, type);
    }

    public StringEncodedColumnReader openStringReader(int rowCount, int pageSize, Decompressor decompressor, String path, String name, boolean treeDict, VarcharType type)
    {
        ByteBuffer mapped = openFile(path, name, ".tar");
        int dataSize = mapped.getInt(mapped.limit() - Integer.BYTES);
        mapped.position(mapped.limit() - Integer.BYTES - dataSize);
        ByteBuffer data = mapped.slice();
        data.limit(dataSize);

        int sstSize = mapped.getInt(mapped.limit() - 2 * Integer.BYTES - dataSize);
        mapped.position(mapped.limit() - 2 * Integer.BYTES - dataSize - sstSize);
        ByteBuffer sst = mapped.slice();
        sst.limit(sstSize);

        if (treeDict) {
            ByteBuffer tree = openFile(path, name, ".dict");
            StringDictionary dict = ImmutableTrieTree.decode(tree, sst);
            return StringEncodedColumnReader.decode(rowCount, pageSize, type, decompressor, data, dict);
        }
        else {
            StringDictionary dict = SstDictionary.decode(sst);
            return StringEncodedColumnReader.decode(rowCount, pageSize, type, decompressor, data, dict);
        }
    }

    @Deprecated
    public CStoreColumnReader openIntReader(String path, String name, IntegerType type)
    {
        ByteBuffer buffer = openFile(path, name, ".bin");
        IntBuffer intBuffer = buffer.asIntBuffer();
        return new IntColumnPlainReader(intBuffer);
    }

    public IntColumnZipReader openIntZipReader(String path, String name, IntegerType type,
            int rowCount, int pageSize, Decompressor decompressor)
    {
        return IntColumnZipReader.decode(rowCount, pageSize, openFile(path, name, ".tar"), decompressor, type);
    }

    @Deprecated
    public LongColumnPlainReader openLongReader(String path, String name, BigintType type)
    {
        return new LongColumnPlainReader(openFile(path, name, ".bin").asLongBuffer());
    }

    public LongColumnZipReader openLongZipReader(String path,
            String name, BigintType type, int rowCount, int pageSize, Decompressor decompressor)
    {
        ByteBuffer file = openFile(path, name, ".tar");
        return LongColumnZipReader.decode(rowCount, pageSize, file, decompressor, type);
    }

    @Deprecated
    public DoubleColumnPlainReader openDoubleReader(String path, String name, DoubleType type)
    {
        return new DoubleColumnPlainReader(openFile(path, name, ".bin").asDoubleBuffer());
    }

    public DoubleColumnZipReader openDoubleZipReader(String path, String name, DoubleType type,
            int rowCount, int pageSize, Decompressor decompressor)
    {
        return DoubleColumnZipReader.decode(rowCount, pageSize, openFile(path, name, ".tar"), decompressor, type);
    }

    public BitmapColumnReader openBitmapReader(String path, String column)
    {
        ByteBuffer buffer = openFile(path, column, ".bitmap");
        return BitmapColumnReader.decode(buffer);
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
