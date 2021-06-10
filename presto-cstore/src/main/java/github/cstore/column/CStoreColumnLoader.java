package github.cstore.column;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.google.common.io.Files;
import io.airlift.compress.Decompressor;
import github.cstore.dictionary.ImmutableTrieTree;
import github.cstore.dictionary.SstDictionary;
import github.cstore.dictionary.StringDictionary;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class CStoreColumnLoader
{
    public CStoreColumnReader.Builder openZipReader(int rowCount, int pageSize, Decompressor decompressor, String path, String column, Type type)
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

    public CStoreColumnReader.Builder openPlainReader(int rowCount, int pageSize, Decompressor decompressor, String path, String column, Type type)
    {
        switch (type.getClass().getSimpleName()) {
            case "IntegerType":
                return openIntPlainReader(path, column, (IntegerType) type);
            case "BigintType":
                return openLongPlainReader(path, column, (BigintType) type);
            case "DoubleType":
                return openDoublePlainReader(path, column, (DoubleType) type);
            case "VarcharType":
                return openStringReader(rowCount, pageSize, decompressor, path, column, (VarcharType) type);
            default:
        }
        throw new UnsupportedOperationException();
    }

    public StringEncodedColumnReader.Builder openStringReader(int rowCount, int pageSize, Decompressor decompressor, String path, String name, VarcharType type)
    {
        return openStringReader(rowCount, pageSize, decompressor, path, name, false, type);
    }

    public StringEncodedColumnReader.Builder openStringReader(int rowCount, int pageSize, Decompressor decompressor, String path, String name, boolean treeDict, VarcharType type)
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
            return StringEncodedColumnReader.newBuilder(rowCount, pageSize, type, decompressor, data, dict);
        }
        else {
            StringDictionary dict = SstDictionary.decode(sst);
            return StringEncodedColumnReader.newBuilder(rowCount, pageSize, type, decompressor, data, dict);
        }
    }

    public CStoreColumnReader.Builder openIntPlainReader(String path, String name, IntegerType type)
    {
        ByteBuffer buffer = openFile(path, name, ".bin");
        return IntColumnPlainReader.builder(buffer);
    }

    public IntColumnZipReader.Builder openIntZipReader(String path, String name, IntegerType type,
            int rowCount, int pageSize, Decompressor decompressor)
    {
        return IntColumnZipReader.newBuilder(rowCount, pageSize, openFile(path, name, ".tar"), decompressor, type);
    }

    public LongColumnPlainReader.Builder openLongPlainReader(String path, String name, BigintType type)
    {
        return LongColumnPlainReader.builder(openFile(path, name, ".bin"));
    }

    public LongColumnZipReader.Builder openLongZipReader(String path,
            String name, BigintType type, int rowCount, int pageSize, Decompressor decompressor)
    {
        ByteBuffer file = openFile(path, name, ".tar");
        return LongColumnZipReader.newBuilder(rowCount, pageSize, file, decompressor, type);
    }

    public DoubleColumnPlainReader.Builder openDoublePlainReader(String path, String name, DoubleType type)
    {
        return new DoubleColumnPlainReader.Builder(openFile(path, name, ".bin"));
    }

    public DoubleColumnZipReader.Builder openDoubleZipReader(String path, String name, DoubleType type,
            int rowCount, int pageSize, Decompressor decompressor)
    {
        return DoubleColumnZipReader.newBuilder(rowCount, pageSize, openFile(path, name, ".tar"), decompressor, type);
    }

    public BitmapColumnReader.Builder openBitmapReader(String path, String column)
    {
        ByteBuffer buffer = openFile(path, column, ".bitmap");
        return BitmapColumnReader.newBuilder(buffer);
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
