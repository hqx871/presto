package com.facebook.presto.cstore;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import io.airlift.compress.Compressor;
import io.airlift.slice.Slice;
import org.apache.cstore.CStoreDatabase;
import org.apache.cstore.coder.CompressFactory;
import org.apache.cstore.column.ChunkColumnWriter;
import org.apache.cstore.column.DoubleColumnPlainWriter;
import org.apache.cstore.column.IntColumnPlainWriter;
import org.apache.cstore.column.LongColumnPlainWriter;
import org.apache.cstore.column.StringEncodedColumnWriter;
import org.apache.cstore.dictionary.MutableTrieTree;
import org.apache.cstore.io.CStoreColumnWriter;
import org.apache.cstore.io.VectorWriterFactory;
import org.apache.cstore.meta.BitmapIndexMeta;
import org.apache.cstore.meta.ColumnMeta;
import org.apache.cstore.meta.TableMeta;
import org.apache.cstore.util.JsonUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

class CStorePageSink
        implements ConnectorPageSink
{
    private static final String compressType = "lz4";

    private final CStoreDatabase database;
    private final HostAddress currentHostAddress;
    private final CStoreTableHandle tableHandle;
    private final List<CStoreColumnHandle> columns;
    private final List<CStoreColumnWriter<?>> columnWriters;
    private final File tableStagingDirectory;
    private final int pageSize;
    private final String metaFile;

    private long addedRows;

    public CStorePageSink(CStoreDatabase database, HostAddress currentHostAddress, CStoreTableHandle tableHandle, List<CStoreColumnHandle> columns)
    {
        this.database = requireNonNull(database, "pagesStore is null");
        this.currentHostAddress = requireNonNull(currentHostAddress, "currentHostAddress is null");
        this.tableHandle = tableHandle;
        this.columns = columns;
        this.tableStagingDirectory = database.getTableStagingPath(tableHandle.getSchema(), tableHandle.getTable());
        this.pageSize = 64 << 10;
        this.metaFile = "meta.json";
        Compressor compressor = CompressFactory.INSTANCE.getCompressor(compressType);
        this.columnWriters = createColumnWriter(tableStagingDirectory, columns, pageSize, compressor);
    }

    private static List<CStoreColumnWriter<?>> createColumnWriter(File tableDirectory, List<CStoreColumnHandle> columns, int pageSize, Compressor compressor)
    {
        List<CStoreColumnWriter<?>> writers = new ArrayList<>(columns.size());
        for (CStoreColumnHandle columnHandle : columns) {
            String type = columnHandle.getColumnType().getTypeSignature().getBase().toLowerCase(Locale.getDefault());
            String name = columnHandle.getColumnName();
            VectorWriterFactory binWriterFactory = new VectorWriterFactory(tableDirectory.getAbsolutePath(), name, "bin");
            VectorWriterFactory zipWriterFactory = new VectorWriterFactory(tableDirectory.getAbsolutePath(), name, "tar");
            switch (type) {
                case "integer":
                    writers.add(new ChunkColumnWriter<>(pageSize, compressor, zipWriterFactory, new IntColumnPlainWriter(binWriterFactory, false), false));
                    break;
                case "bigint":
                    writers.add(new ChunkColumnWriter<>(pageSize, compressor, zipWriterFactory, new LongColumnPlainWriter(binWriterFactory, false), false));
                    break;
                case "double":
                    writers.add(new ChunkColumnWriter<>(pageSize, compressor, zipWriterFactory, new DoubleColumnPlainWriter(binWriterFactory, false), false));
                    break;
                case "varchar":
                    StringEncodedColumnWriter stringEncodedVectorWriter = new StringEncodedColumnWriter(pageSize, compressor, new MutableTrieTree(), zipWriterFactory, false, false);
                    writers.add(stringEncodedVectorWriter);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
        return writers;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        for (int i = 0; i < page.getChannelCount(); i++) {
            CStoreColumnWriter<?> writer = columnWriters.get(i);
            Block block = page.getBlock(i);
            writer.write(block, page.getPositionCount());
        }
        addedRows += page.getPositionCount();
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        flushTableData();
        generateTableMeta();
        try {
            database.commitStagingTable(tableHandle.getSchema(), tableHandle.getTable());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return completedFuture(ImmutableList.of(new CStoreDataFragment(currentHostAddress, addedRows).toSlice()));
    }

    private void flushTableData()
    {
        try {
            for (CStoreColumnWriter<?> columnWriter : columnWriters) {
                columnWriter.close();
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void generateTableMeta()
    {
        int columnCnt = columns.size();
        List<ColumnMeta> columns = new ArrayList<>();
        List<BitmapIndexMeta> bitmapIndexes = new ArrayList<>();
        for (int i = 0; i < columnCnt; i++) {
            CStoreColumnHandle columnHandle = this.columns.get(i);
            String type = columnHandle.getColumnType().getTypeSignature().getBase().toLowerCase(Locale.getDefault());
            ColumnMeta columnMeta = new ColumnMeta();
            columnMeta.setVersion("v1");
            columnMeta.setName(columnHandle.getColumnName());
            columnMeta.setTypeName(type);
            columnMeta.setFileName(columnHandle.getColumnName() + ".tar");
            columnMeta.setCompressType(compressType);
            columns.add(columnMeta);

            if ("varchar".equals(type)) {
                BitmapIndexMeta indexMeta = new BitmapIndexMeta();
                indexMeta.setName(columnHandle.getColumnName());
                indexMeta.setFileName(columnHandle.getColumnName() + ".bitmap");
                indexMeta.setCardinality(columnMeta.getCardinality());
                columnMeta.setDictionaryEncode(true);
                bitmapIndexes.add(indexMeta);
            }
        }

        TableMeta tableMeta = new TableMeta();
        tableMeta.setName(tableHandle.getTable());
        tableMeta.setColumns(columns);
        tableMeta.setRowCnt((int) addedRows);
        tableMeta.setBitmapIndexes(bitmapIndexes);
        tableMeta.setPageSize(pageSize);

        try {
            Files.write(Paths.get(tableStagingDirectory.getAbsolutePath(), metaFile), JsonUtil.write(tableMeta));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abort()
    {
    }
}
