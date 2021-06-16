package com.facebook.presto.cstore.storage;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import github.cstore.column.BitmapColumnReader;
import github.cstore.column.CStoreColumnReader;
import github.cstore.column.ColumnChunkZipReader;
import github.cstore.column.DoubleColumnReaderFactory;
import github.cstore.column.IntColumnReaderFactory;
import github.cstore.column.LongColumnReaderFactory;
import github.cstore.column.StringEncodedColumnReader;
import github.cstore.dictionary.SstDictionary;
import github.cstore.dictionary.StringDictionary;
import io.airlift.compress.Decompressor;

import java.nio.ByteBuffer;
import java.util.Locale;

public class CStoreColumnLoader
{
    public CStoreColumnReader.Builder openZipReader(int rowCount, int pageRowCount, Decompressor decompressor, ByteBuffer buffer, Type type)
    {
        switch (type.getTypeSignature().getBase().toLowerCase(Locale.getDefault())) {
            case "date":
            case "integer":
                return openIntZipReader(buffer, type, rowCount, pageRowCount, decompressor);
            case "timestamp":
            case "bigint":
                return openLongZipReader(buffer, type, rowCount, pageRowCount, decompressor);
            case "double":
                return openDoubleZipReader(buffer, type, rowCount, pageRowCount, decompressor);
            case "varchar":
                return openStringReader(rowCount, pageRowCount, decompressor, buffer, (VarcharType) type);
            default:
        }
        throw new UnsupportedOperationException();
    }

    public StringEncodedColumnReader.Builder openStringReader(int rowCount, int pageRowCount, Decompressor decompressor, ByteBuffer mapped, VarcharType type)
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
        return StringEncodedColumnReader.newBuilder(rowCount, pageRowCount, type, decompressor, data, dict);
    }

    public ColumnChunkZipReader.Builder openIntZipReader(ByteBuffer buffer, Type type,
            int rowCount, int pageRowCount, Decompressor decompressor)
    {
        IntColumnReaderFactory plainBuilder = new IntColumnReaderFactory();
        return ColumnChunkZipReader.newBuilder(rowCount, pageRowCount, buffer, decompressor, type, true, plainBuilder);
    }

    public ColumnChunkZipReader.Builder openLongZipReader(ByteBuffer buffer, Type type, int rowCount, int pageRowCount, Decompressor decompressor)
    {
        LongColumnReaderFactory plainBuilder = new LongColumnReaderFactory();
        return ColumnChunkZipReader.newBuilder(rowCount, pageRowCount, buffer, decompressor, type, true, plainBuilder);
    }

    public ColumnChunkZipReader.Builder openDoubleZipReader(ByteBuffer buffer, Type type,
            int rowCount, int pageRowCount, Decompressor decompressor)
    {
        DoubleColumnReaderFactory plainBuilder = new DoubleColumnReaderFactory();
        return ColumnChunkZipReader.newBuilder(rowCount, pageRowCount, buffer, decompressor, type, true, plainBuilder);
    }

    public BitmapColumnReader.Builder openBitmapReader(ByteBuffer buffer)
    {
        return BitmapColumnReader.newBuilder(buffer);
    }
}
