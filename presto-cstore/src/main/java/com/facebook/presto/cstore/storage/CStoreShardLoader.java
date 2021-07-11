package com.facebook.presto.cstore.storage;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.google.common.io.Files;
import github.cstore.coder.CompressFactory;
import github.cstore.column.BitmapColumnReader;
import github.cstore.column.CStoreColumnReader;
import github.cstore.meta.ShardColumn;
import github.cstore.meta.ShardSchema;
import io.airlift.compress.Decompressor;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

public class CStoreShardLoader
{
    private static final JsonCodec<ShardSchema> SHARD_SCHEMA_CODEC = JsonCodec.jsonCodec(ShardSchema.class);

    private final File path;
    private final Map<Long, CStoreColumnReader.Supplier> columnReaderSuppliers;
    private final Map<Long, BitmapColumnReader.Supplier> bitmapReaderSuppliers;
    private ShardSchema shardSchema;
    private final CompressFactory compressFactory;
    private final TypeManager typeManager;

    public CStoreShardLoader(File path, CompressFactory compressFactory, TypeManager typeManager)
    {
        this.path = path;
        this.compressFactory = compressFactory;
        this.typeManager = typeManager;
        this.columnReaderSuppliers = new HashMap<>();
        this.bitmapReaderSuppliers = new HashMap<>();
    }

    public void setup()
            throws IOException
    {
        CStoreColumnLoader columnLoader = new CStoreColumnLoader();
        ByteBuffer buffer = Files.map(path, FileChannel.MapMode.READ_ONLY);
        long actualChecksum = buffer.getLong(buffer.limit() - Long.BYTES);
        buffer.limit(buffer.limit() - Long.BYTES);
        buffer.mark();
        CRC32 crc32 = new CRC32();
        crc32.update(buffer);
        long expectChecksum = crc32.getValue();
        buffer.reset();

        assert expectChecksum == actualChecksum : "crc32 checksum error";
        assert buffer.getShort() == 'H' : "magic error";
        assert buffer.getInt() == 1 : "version error";
        buffer = buffer.slice();

        int metaJsonSize = buffer.getInt(buffer.limit() - Integer.BYTES);
        byte[] metaBytes = new byte[metaJsonSize];
        buffer.position(buffer.limit() - Integer.BYTES - metaJsonSize);
        buffer.get(metaBytes, 0, metaJsonSize);
        shardSchema = SHARD_SCHEMA_CODEC.fromJson(metaBytes);
        int columnOffset = 0;
        for (ShardColumn shardColumn : shardSchema.getColumns()) {
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
                BitmapColumnReader.Supplier builder = columnLoader.openBitmapReader(bitmapBuffer);
                bitmapReaderSuppliers.put(shardColumn.getColumnId(), builder);
                columnBuffer.position(0);
                columnBuffer.limit(columnBuffer.limit() - Integer.BYTES - bitmapSize);
            }
            CStoreColumnReader.Supplier columnSupplier = columnLoader.openZipReader(shardSchema.getRowCount(), decompressor, columnBuffer, type);
            columnReaderSuppliers.put(shardColumn.getColumnId(), columnSupplier);
            columnOffset += shardColumn.getByteSize();
        }
    }

    public ShardSchema getShardSchema()
    {
        return shardSchema;
    }

    public Map<Long, CStoreColumnReader.Supplier> getColumnReaderSuppliers()
    {
        return columnReaderSuppliers;
    }

    public Map<Long, BitmapColumnReader.Supplier> getBitmapReaderSuppliers()
    {
        return bitmapReaderSuppliers;
    }

    private Type getType(String base)
    {
        return typeManager.getType(TypeSignature.parseTypeSignature(base));
    }

    public void close()
    {
    }
}
