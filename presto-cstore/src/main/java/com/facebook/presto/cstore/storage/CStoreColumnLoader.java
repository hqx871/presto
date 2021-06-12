package com.facebook.presto.cstore.storage;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import github.cstore.column.BitmapColumnReader;
import github.cstore.column.CStoreColumnReader;
import github.cstore.column.DoubleColumnPlainReader;
import github.cstore.column.DoubleColumnZipReader;
import github.cstore.column.IntColumnPlainReader;
import github.cstore.column.IntColumnZipReader;
import github.cstore.column.LongColumnPlainReader;
import github.cstore.column.LongColumnZipReader;
import github.cstore.column.StringEncodedColumnReader;
import github.cstore.dictionary.SstDictionary;
import github.cstore.dictionary.StringDictionary;
import io.airlift.compress.Decompressor;

import java.nio.ByteBuffer;
import java.util.Locale;

public class CStoreColumnLoader
{
    public CStoreColumnReader.Builder openZipReader(int rowCount, int pageSize, Decompressor decompressor, ByteBuffer buffer, Type type)
    {
        switch (type.getTypeSignature().getBase().toLowerCase(Locale.getDefault())) {
            case "date":
            case "integer":
                return openIntZipReader(buffer, (IntegerType) type, rowCount, pageSize, decompressor);
            case "timestamp":
            case "bigint":
                return openLongZipReader(buffer, type, rowCount, pageSize, decompressor);
            case "double":
                return openDoubleZipReader(buffer, (DoubleType) type, rowCount, pageSize, decompressor);
            case "varchar":
                return openStringReader(rowCount, pageSize, decompressor, buffer, (VarcharType) type);
            default:
        }
        throw new UnsupportedOperationException();
    }

    public CStoreColumnReader.Builder openPlainReader(int rowCount, int pageSize, Decompressor decompressor, ByteBuffer buffer, Type type)
    {
        switch (type.getTypeSignature().getBase().toLowerCase(Locale.getDefault())) {
            case "integer":
                return openIntPlainReader(buffer, (IntegerType) type);
            case "timestamp":
            case "bigint":
                return openLongPlainReader(buffer, (BigintType) type);
            case "double":
                return openDoublePlainReader(buffer, (DoubleType) type);
            case "varchar":
                return openStringReader(rowCount, pageSize, decompressor, buffer, (VarcharType) type);
            default:
        }
        throw new UnsupportedOperationException();
    }

    public StringEncodedColumnReader.Builder openStringReader(int rowCount, int pageSize, Decompressor decompressor, ByteBuffer mapped, VarcharType type)
    {
        int dataSize = mapped.getInt(mapped.limit() - Integer.BYTES);
        mapped.position(mapped.limit() - Integer.BYTES - dataSize);
        ByteBuffer data = mapped.slice();
        data.limit(dataSize);

        int sstSize = mapped.getInt(mapped.limit() - 2 * Integer.BYTES - dataSize);
        mapped.position(mapped.limit() - 2 * Integer.BYTES - dataSize - sstSize);
        ByteBuffer sst = mapped.slice();
        sst.limit(sstSize);

        StringDictionary dict = SstDictionary.decode(sst);
        return StringEncodedColumnReader.newBuilder(rowCount, pageSize, type, decompressor, data, dict);
    }

    public CStoreColumnReader.Builder openIntPlainReader(ByteBuffer buffer, IntegerType type)
    {
        return IntColumnPlainReader.builder(buffer);
    }

    public IntColumnZipReader.Builder openIntZipReader(ByteBuffer buffer, IntegerType type,
            int rowCount, int pageSize, Decompressor decompressor)
    {
        return IntColumnZipReader.newBuilder(rowCount, pageSize, buffer, decompressor, type);
    }

    public LongColumnPlainReader.Builder openLongPlainReader(ByteBuffer buffer, BigintType type)
    {
        return LongColumnPlainReader.builder(buffer);
    }

    public LongColumnZipReader.Builder openLongZipReader(ByteBuffer buffer, Type type, int rowCount, int pageSize, Decompressor decompressor)
    {
        return LongColumnZipReader.newBuilder(rowCount, pageSize, buffer, decompressor, type);
    }

    public DoubleColumnPlainReader.Builder openDoublePlainReader(ByteBuffer buffer, DoubleType type)
    {
        return new DoubleColumnPlainReader.Builder(buffer);
    }

    public DoubleColumnZipReader.Builder openDoubleZipReader(ByteBuffer file, DoubleType type,
            int rowCount, int pageSize, Decompressor decompressor)
    {
        return DoubleColumnZipReader.newBuilder(rowCount, pageSize, file, decompressor, type);
    }

    public BitmapColumnReader.Builder openBitmapReader(ByteBuffer buffer)
    {
        return BitmapColumnReader.newBuilder(buffer);
    }
}
