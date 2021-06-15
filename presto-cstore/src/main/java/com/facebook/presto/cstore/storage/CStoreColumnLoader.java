package com.facebook.presto.cstore.storage;

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

    public CStoreColumnReader.Builder openPlainReader(int rowCount, int pageRowCount, Decompressor decompressor, ByteBuffer buffer, Type type)
    {
        switch (type.getTypeSignature().getBase().toLowerCase(Locale.getDefault())) {
            case "integer":
                return openIntPlainReader(buffer, type);
            case "timestamp":
            case "bigint":
                return openLongPlainReader(buffer, type);
            case "double":
                return openDoublePlainReader(buffer, type);
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

    public CStoreColumnReader.Builder openIntPlainReader(ByteBuffer buffer, Type type)
    {
        return IntColumnPlainReader.builder(buffer);
    }

    public IntColumnZipReader.Builder openIntZipReader(ByteBuffer buffer, Type type,
            int rowCount, int pageRowCount, Decompressor decompressor)
    {
        return IntColumnZipReader.newBuilder(rowCount, pageRowCount, buffer, decompressor, type, true);
    }

    public LongColumnPlainReader.Builder openLongPlainReader(ByteBuffer buffer, Type type)
    {
        return LongColumnPlainReader.builder(buffer);
    }

    public LongColumnZipReader.Builder openLongZipReader(ByteBuffer buffer, Type type, int rowCount, int pageRowCount, Decompressor decompressor)
    {
        return LongColumnZipReader.newBuilder(rowCount, pageRowCount, buffer, decompressor, type, true);
    }

    public DoubleColumnPlainReader.Builder openDoublePlainReader(ByteBuffer buffer, Type type)
    {
        return new DoubleColumnPlainReader.Builder(buffer);
    }

    public DoubleColumnZipReader.Builder openDoubleZipReader(ByteBuffer file, Type type,
            int rowCount, int pageRowCount, Decompressor decompressor)
    {
        return DoubleColumnZipReader.newBuilder(rowCount, pageRowCount, file, decompressor, type, true);
    }

    public BitmapColumnReader.Builder openBitmapReader(ByteBuffer buffer)
    {
        return BitmapColumnReader.newBuilder(buffer);
    }
}
