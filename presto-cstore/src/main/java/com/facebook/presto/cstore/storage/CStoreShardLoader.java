package com.facebook.presto.cstore.storage;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.google.common.io.Files;
import github.cstore.coder.CompressFactory;
import github.cstore.column.BitmapColumnReader;
import github.cstore.column.CStoreColumnReader;
import github.cstore.meta.ShardColumn;
import github.cstore.meta.ShardMeta;
import github.cstore.util.JsonUtil;
import io.airlift.compress.Decompressor;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

public class CStoreShardLoader
{
    private final File path;
    private final Map<Long, CStoreColumnReader.Builder> columnReaderMap;
    private final Map<Long, BitmapColumnReader.Builder> bitmapReaderMap;
    private ShardMeta shardMeta;
    private final CompressFactory compressFactory;

    public CStoreShardLoader(File path, CompressFactory compressFactory)
    {
        this.path = path;
        this.compressFactory = compressFactory;
        this.columnReaderMap = new HashMap<>();
        this.bitmapReaderMap = new HashMap<>();
    }

    public void setup()
            throws IOException
    {
        CStoreColumnLoader columnLoader = new CStoreColumnLoader();
        ByteBuffer buffer = Files.map(path, FileChannel.MapMode.READ_ONLY);
        int metaJsonSize = buffer.getInt(buffer.limit() - Integer.BYTES);
        byte[] metaBytes = new byte[metaJsonSize];
        buffer.position(buffer.limit() - Integer.BYTES - metaJsonSize);
        buffer.get(metaBytes, 0, metaJsonSize);
        shardMeta = JsonUtil.read(metaBytes, ShardMeta.class);
        int columnOffset = 0;
        buffer.position(0);
        for (ShardColumn shardColumn : shardMeta.getColumns()) {
            Decompressor decompressor = compressFactory.getDecompressor(shardColumn.getCompressType());
            Type type = getType(shardColumn.getTypeName());
            buffer.position(columnOffset);
            ByteBuffer columnBuffer = buffer.slice();
            columnBuffer.limit(shardColumn.getByteSize());
            if (shardColumn.isHasBitmap()) {
                int bitmapSize = columnBuffer.getInt(columnBuffer.limit() - Integer.BYTES);
                columnBuffer.position(columnBuffer.limit() - Integer.BYTES - bitmapSize);
                ByteBuffer bitmapBuffer = columnBuffer.slice();
                bitmapBuffer.limit(bitmapSize);
                BitmapColumnReader.Builder builder = columnLoader.openBitmapReader(bitmapBuffer);
                bitmapReaderMap.put(shardColumn.getColumnId(), builder);
                columnBuffer.position(0);
                columnBuffer.limit(columnBuffer.limit() - Integer.BYTES - bitmapSize);
            }
            CStoreColumnReader.Builder columnBuilder = columnLoader.openZipReader(shardMeta.getRowCnt(), shardMeta.getPageSize(),
                    decompressor, columnBuffer, type);
            columnReaderMap.put(shardColumn.getColumnId(), columnBuilder);
            columnOffset += shardColumn.getByteSize();
        }
    }

    public ShardMeta getShardMeta()
    {
        return shardMeta;
    }

    public Map<Long, CStoreColumnReader.Builder> getColumnReaderMap()
    {
        return columnReaderMap;
    }

    public Map<Long, BitmapColumnReader.Builder> getBitmapReaderMap()
    {
        return bitmapReaderMap;
    }

    public static Type getType(String base)
    {
        switch (base) {
            case "integer":
            case "int":
                return IntegerType.INTEGER;
            case "bigint":
            case "long":
                return BigintType.BIGINT;
            case "double":
                return DoubleType.DOUBLE;
            case "varchar":
            case "string":
                return VarcharType.VARCHAR;
            case "timestamp":
                return TimestampType.TIMESTAMP;
            default:
        }
        throw new UnsupportedOperationException();
    }
}
