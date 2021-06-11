package com.facebook.presto.cstore.storage;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.io.DataOutput;
import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;
import github.cstore.coder.CompressFactory;
import github.cstore.column.CStoreColumnWriter;
import github.cstore.column.ChunkColumnWriter;
import github.cstore.column.DoubleColumnPlainWriter;
import github.cstore.column.IntColumnPlainWriter;
import github.cstore.column.LongColumnPlainWriter;
import github.cstore.column.StringEncodedColumnWriter;
import github.cstore.dictionary.MutableTrieTree;
import github.cstore.io.FileStreamWriterFactory;
import github.cstore.io.StreamWriterFactory;
import github.cstore.meta.ShardColumn;
import github.cstore.meta.ShardMeta;
import github.cstore.util.JsonUtil;
import io.airlift.compress.Compressor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.spi.ConnectorPageSink.NOT_BLOCKED;

public class CStoreWriter
{
    private static final String compressType = "lz4";

    private final long[] columnIds;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final List<CStoreColumnWriter<?>> columnWriters;
    private final File tableStagingDirectory;
    private final int pageSize;
    private final DataSink sink;

    private int addedRows;
    private boolean closed;
    private final UUID shardUuid;

    public CStoreWriter(List<Long> columnIds, File stagingDirectory, DataSink sink,
            List<String> columnNames, List<Type> columnTypes, UUID shardUuid)
    {
        this.columnIds = columnIds.stream().mapToLong(i -> i).toArray();
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.tableStagingDirectory = stagingDirectory;
        this.sink = sink;
        this.shardUuid = shardUuid;
        this.pageSize = 64 << 10;
        Compressor compressor = CompressFactory.INSTANCE.getCompressor(compressType);
        this.columnWriters = createColumnWriter(tableStagingDirectory, columnNames, columnTypes, pageSize, compressor);
    }

    private static List<CStoreColumnWriter<?>> createColumnWriter(File tableDirectory, List<String> columnNames, List<Type> columnTypes, int pageSize, Compressor compressor)
    {
        StreamWriterFactory writerFactory = new FileStreamWriterFactory(tableDirectory);
        List<CStoreColumnWriter<?>> writers = new ArrayList<>(columnNames.size());
        for (int i = 0; i < columnNames.size(); i++) {
            String type = columnTypes.get(i).getTypeSignature().getBase().toLowerCase(Locale.getDefault());
            String name = columnNames.get(i);
            switch (type) {
                case "integer":
                    writers.add(new ChunkColumnWriter<>(name, pageSize, compressor, writerFactory, new IntColumnPlainWriter(name, writerFactory, false), false));
                    break;
                case "timestamp":
                case "bigint":
                    writers.add(new ChunkColumnWriter<>(name, pageSize, compressor, writerFactory, new LongColumnPlainWriter(name, writerFactory, false), false));
                    break;
                case "double":
                    writers.add(new ChunkColumnWriter<>(name, pageSize, compressor, writerFactory, new DoubleColumnPlainWriter(name, writerFactory, false), false));
                    break;
                case "varchar":
                    StringEncodedColumnWriter stringEncodedVectorWriter = new StringEncodedColumnWriter(name, pageSize, compressor, new MutableTrieTree(), writerFactory, false, false);
                    writers.add(stringEncodedVectorWriter);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
        return writers;
    }

    //@Override
    public CompletableFuture<?> write(Page page)
            throws IOException
    {
        for (int i = 0; i < page.getChannelCount(); i++) {
            CStoreColumnWriter<?> writer = columnWriters.get(i);
            Block block = page.getBlock(i);
            writer.write(block, page.getPositionCount());
        }
        addedRows += page.getPositionCount();
        return NOT_BLOCKED;
    }

    //@Override
    public void close()
            throws IOException
    {
        if (!closed) {
            flushTableData();
            for (CStoreColumnWriter<?> columnWriter : columnWriters) {
                columnWriter.close();
            }
        }
        closed = true;
    }

    private void flushTableData()
    {
        try {
            int[] columnBytesSize = new int[columnNames.size()];
            for (int i = 0; i < columnWriters.size(); i++) {
                CStoreColumnWriter<?> columnWriter = columnWriters.get(i);
                columnWriter.flush();
                Slice columnSlice = Slices.wrappedBuffer(columnWriter.mapFile());
                DataOutput columnData = DataOutput.createDataOutput(columnSlice);
                sink.write(ImmutableList.of(columnData));
                columnBytesSize[i] = columnSlice.length();
            }
            ShardMeta shardMeta = generateMeta(columnBytesSize);
            Slice shardMetaBytes = Slices.wrappedBuffer(JsonUtil.write(shardMeta));
            sink.write(ImmutableList.of(DataOutput.createDataOutput(shardMetaBytes)));
            sink.write(ImmutableList.of(DataOutput.createDataOutput(Slices.wrappedIntArray(Integer.reverseBytes(shardMetaBytes.length())))));
            sink.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ShardMeta generateMeta(int[] columnBytesSize)
    {
        List<ShardColumn> columns = new ArrayList<>();
        for (int i = 0; i < columnIds.length; i++) {
            String type = columnTypes.get(i).getTypeSignature().getBase().toLowerCase(Locale.getDefault());
            ShardColumn columnMeta = new ShardColumn();
            columnMeta.setVersion("v1");
            columnMeta.setColumnId(columnIds[i]);
            columnMeta.setTypeName(type);
            columnMeta.setFileName(columnNames.get(i) + ".tar");
            columnMeta.setCompressType(compressType);
            columnMeta.setByteSize(columnBytesSize[i]);
            columnMeta.setHasBitmap("varchar".equals(type));
            columns.add(columnMeta);
        }

        ShardMeta shardMeta = new ShardMeta();
        shardMeta.setColumns(columns);
        shardMeta.setRowCnt(addedRows);
        shardMeta.setPageSize(pageSize);

        return shardMeta;
    }
}
