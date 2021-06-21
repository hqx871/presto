package com.facebook.presto.cstore.storage;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.io.DataOutput;
import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import github.cstore.coder.CompressFactory;
import github.cstore.column.CStoreColumnWriter;
import github.cstore.column.ColumnChunkZipWriter;
import github.cstore.column.DoubleColumnPlainWriter;
import github.cstore.column.IntColumnPlainWriter;
import github.cstore.column.LongColumnPlainWriter;
import github.cstore.column.NullableColumnWriter;
import github.cstore.column.StringEncodedColumnWriter;
import github.cstore.dictionary.MutableTrieTree;
import github.cstore.io.FileStreamWriterFactory;
import github.cstore.io.MemoryStreamWriterFactory;
import github.cstore.io.StreamWriterFactory;
import github.cstore.meta.ShardColumn;
import github.cstore.meta.ShardSchema;
import io.airlift.compress.Compressor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.zip.CRC32;

import static com.facebook.presto.cstore.CStoreErrorCode.CSTORE_WRITER_DATA_ERROR;
import static com.facebook.presto.spi.ConnectorPageSink.NOT_BLOCKED;
import static com.google.common.base.Preconditions.checkArgument;

public class CStoreShardFileWriter
        implements ShardFileWriter
{
    private static final String COMPRESS_TYPE = "lz4";
    private static final JsonCodec<ShardSchema> SHARD_SCHEMA_CODEC = JsonCodec.jsonCodec(ShardSchema.class);

    private final long[] columnIds;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final List<CStoreColumnWriter<?>> columnWriters;
    private final File tableStagingDirectory;
    private final DataSink sink;

    private int rowCount;
    private boolean closed;
    //private long rowCount;
    private long uncompressedSize;

    public CStoreShardFileWriter(List<Long> columnIds, File stagingDirectory, DataSink sink,
            List<String> columnNames, List<Type> columnTypes)
    {
        checkArgument(isUnique(columnIds), "ids must be unique");

        this.columnIds = columnIds.stream().mapToLong(i -> i).toArray();
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.tableStagingDirectory = stagingDirectory;
        this.sink = sink;
        Compressor compressor = CompressFactory.INSTANCE.getCompressor(COMPRESS_TYPE);
        this.columnWriters = createColumnWriter(tableStagingDirectory, columnNames, columnTypes, compressor);
    }

    public void setup()
    {
        columnWriters.forEach(CStoreColumnWriter::setup);
    }

    @Override
    public void appendPages(List<Page> pages)
    {
        for (Page page : pages) {
            try {
                this.appendPage(page);
            }
            catch (IOException | UncheckedIOException e) {
                throw new PrestoException(CSTORE_WRITER_DATA_ERROR, e);
            }
            uncompressedSize += page.getLogicalSizeInBytes();
            rowCount += page.getPositionCount();
        }
    }

    @Override
    public void appendPages(List<Page> inputPages, int[] pageIndexes, int[] positionIndexes)
    {
        checkArgument(pageIndexes.length == positionIndexes.length, "pageIndexes and positionIndexes do not match");
        for (int i = 0; i < pageIndexes.length; i++) {
            Page page = inputPages.get(pageIndexes[i]);
            // This will do data copy; be aware
            Page singleValuePage = page.getSingleValuePage(positionIndexes[i]);
            try {
                this.appendPage(singleValuePage);
                uncompressedSize += singleValuePage.getLogicalSizeInBytes();
                rowCount++;
            }
            catch (IOException | UncheckedIOException e) {
                throw new PrestoException(CSTORE_WRITER_DATA_ERROR, e);
            }
        }
    }

    @Override
    public long getRowCount()
    {
        return rowCount;
    }

    @Override
    public long getUncompressedSize()
    {
        return uncompressedSize;
    }

    private static List<CStoreColumnWriter<?>> createColumnWriter(File tableDirectory, List<String> columnNames, List<Type> columnTypes, Compressor compressor)
    {
        StreamWriterFactory fileStreamWriterFactory = new FileStreamWriterFactory(tableDirectory);
        StreamWriterFactory memoryWriterFactory = new MemoryStreamWriterFactory();
        List<CStoreColumnWriter<?>> writers = new ArrayList<>(columnNames.size());
        int pageSize = 64 << 10;
        for (int i = 0; i < columnNames.size(); i++) {
            String type = columnTypes.get(i).getTypeSignature().getBase().toLowerCase(Locale.getDefault());
            String name = columnNames.get(i);
            switch (type) {
                case "date":
                case "integer":
                    IntColumnPlainWriter intWriter = new IntColumnPlainWriter(name, memoryWriterFactory.createWriter(name + ".plain", true), true);
                    NullableColumnWriter<Integer> intNullableWriter = new NullableColumnWriter<>(name, memoryWriterFactory.createWriter(name + ".nullable", true), intWriter, true);
                    writers.add(new ColumnChunkZipWriter<>(name, pageSize / Integer.BYTES, compressor, fileStreamWriterFactory.createWriter(name + ".tar", false), memoryWriterFactory, intNullableWriter, false));
                    break;
                case "timestamp":
                case "bigint":
                    LongColumnPlainWriter longColumnPlainWriter = new LongColumnPlainWriter(name, memoryWriterFactory.createWriter(name + ".plain", true), true);
                    NullableColumnWriter<Long> longNullableColumnWriter = new NullableColumnWriter<>(name, fileStreamWriterFactory.createWriter(name + ".nullable", true), longColumnPlainWriter, true);
                    writers.add(new ColumnChunkZipWriter<>(name, pageSize / Long.BYTES, compressor, fileStreamWriterFactory.createWriter(name + ".tar", false), memoryWriterFactory, longNullableColumnWriter, false));
                    break;
                case "double":
                    DoubleColumnPlainWriter doubleColumnPlainWriter = new DoubleColumnPlainWriter(name, memoryWriterFactory.createWriter(name + ".plain", true), true);
                    NullableColumnWriter<Double> doubleNullableColumnWriter = new NullableColumnWriter<>(name, fileStreamWriterFactory.createWriter(name + ".nullable", true), doubleColumnPlainWriter, true);
                    writers.add(new ColumnChunkZipWriter<>(name, pageSize / Double.BYTES, compressor, fileStreamWriterFactory.createWriter(name + ".tar", false), memoryWriterFactory, doubleNullableColumnWriter, false));
                    break;
                case "varchar":
                    //todo get write index from ddl.
                    StringEncodedColumnWriter stringEncodedVectorWriter = new StringEncodedColumnWriter(name, pageSize, compressor, new MutableTrieTree(),
                            fileStreamWriterFactory.createWriter(name + ".tar", false), memoryWriterFactory, true, false);
                    writers.add(stringEncodedVectorWriter);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
        return writers;
    }

    //@Override
    public CompletableFuture<?> appendPage(Page page)
            throws IOException
    {
        for (int i = 0; i < page.getChannelCount(); i++) {
            CStoreColumnWriter writer = columnWriters.get(i);
            Block block = page.getBlock(i);
            for (int j = 0; j < page.getPositionCount(); j++) {
                Object value = writer.readValue(block, j);
                writer.write(value);
            }
        }
        rowCount += page.getPositionCount();
        uncompressedSize += page.getLogicalSizeInBytes();
        return NOT_BLOCKED;
    }

    public CompletableFuture<?> appendPage(Page page, int[] positions, int size)
            throws IOException
    {
        for (int i = 0; i < page.getChannelCount(); i++) {
            CStoreColumnWriter writer = columnWriters.get(i);
            Block block = page.getBlock(i);
            for (int j = 0; j < size; j++) {
                Object value = writer.readValue(block, positions[j]);
                writer.write(value);
            }
        }
        rowCount += page.getPositionCount();
        uncompressedSize += page.getLogicalSizeInBytes();
        return NOT_BLOCKED;
    }

    //@Override
    public void close()
            throws IOException
    {
        if (!closed) {
            flushData();
            for (CStoreColumnWriter<?> columnWriter : columnWriters) {
                columnWriter.close();
            }
        }
        closed = true;
    }

    private void flushData()
            throws IOException
    {
        CRC32 crc32 = new CRC32();
        ByteBuffer header = ByteBuffer.allocate(Short.BYTES + Integer.BYTES);
        header.putShort((short) 'H');
        header.putInt(1);
        header.flip();
        header.mark();
        sink.write(ImmutableList.of(DataOutput.createDataOutput(Slices.wrappedBuffer(header))));
        header.reset();
        crc32.update(header);
        int[] columnBytesSize = new int[columnNames.size()];
        for (int i = 0; i < columnWriters.size(); i++) {
            CStoreColumnWriter<?> columnWriter = columnWriters.get(i);
            ByteBuffer columnBuffer = columnWriter.mapBuffer();
            Slice columnSlice = Slices.wrappedBuffer(columnBuffer);
            DataOutput columnData = DataOutput.createDataOutput(columnSlice);
            columnBuffer.mark();
            sink.write(ImmutableList.of(columnData));
            columnBuffer.reset();
            crc32.update(columnBuffer);
            columnBytesSize[i] = columnSlice.length();
        }
        ShardSchema shardSchema = generateMeta(columnBytesSize);
        byte[] shardSchemaBytes = SHARD_SCHEMA_CODEC.toJsonBytes(shardSchema);
        ByteBuffer footer = ByteBuffer.allocate(shardSchemaBytes.length + Integer.BYTES);
        footer.put(shardSchemaBytes);
        footer.putInt(shardSchemaBytes.length);
        footer.flip();

        footer.mark();
        sink.write(ImmutableList.of(DataOutput.createDataOutput(Slices.wrappedBuffer(footer))));
        footer.reset();

        crc32.update(footer);
        long crc32Checksum = crc32.getValue();
        sink.write(ImmutableList.of(DataOutput.createDataOutput(Slices.wrappedLongArray(Long.reverseBytes(crc32Checksum)))));
        sink.close();
    }

    private ShardSchema generateMeta(int[] columnBytesSize)
    {
        List<ShardColumn> columns = new ArrayList<>();
        for (int i = 0; i < columnIds.length; i++) {
            String type = columnTypes.get(i).getTypeSignature().toString();
            //todo get from ddl
            boolean hasBitmap = "varchar".equalsIgnoreCase(columnTypes.get(i).getTypeSignature().getBase());
            ShardColumn columnMeta = new ShardColumn("v1", columnIds[i], type, columnNames.get(i) + ".tar",
                    -1, hasBitmap, COMPRESS_TYPE, columnBytesSize[i], hasBitmap);
            columns.add(columnMeta);
        }

        return new ShardSchema(columns, rowCount);
    }

    private static <T> boolean isUnique(Collection<T> items)
    {
        return new HashSet<>(items).size() == items.size();
    }
}
