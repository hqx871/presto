package com.facebook.presto.cstore.storage;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.cstore.CStoreColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.base.Stopwatch;
import github.cstore.column.BitmapColumnReader;
import github.cstore.column.CStoreColumnReader;
import github.cstore.column.VectorCursor;
import github.cstore.filter.IndexFilterInterpreter;
import github.cstore.filter.SelectedPositions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class CStorePageSource
        implements ConnectorPageSource
{
    public static final int NULL_COLUMN = -1;
    public static final int ROWID_COLUMN = -2;
    public static final int SHARD_UUID_COLUMN = -3;
    public static final int BUCKET_NUMBER_COLUMN = -4;

    private static final Logger log = Logger.get(CStorePageSource.class);

    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;
    @Nullable
    private final RowExpression filter;
    private Iterator<SelectedPositions> mask;
    private final CStoreColumnReader[] columnReaders;
    private long completedBytes;
    private long completedPositions;
    private long readTimeNanos;
    private long systemMemoryUsage;
    private final int vectorSize;
    private final VectorCursor[] cursors;
    private final StorageManager storageManager;
    private final UUID shardUuid;
    private final int rowCount;
    private final List<CStoreColumnHandle> columnHandles;
    private final TupleDomain<CStoreColumnHandle> predicate;
    private final Map<String, CStoreColumnHandle> columnHandleMap;

    public CStorePageSource(StorageManager storageManager,
            TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution,
            List<CStoreColumnHandle> columnHandles,
            UUID shardUuid,
            @Nullable RowExpression filter,
            int rowCount,
            TupleDomain<CStoreColumnHandle> predicate)
    {
        this.storageManager = storageManager;
        this.typeManager = typeManager;
        this.functionMetadataManager = functionMetadataManager;
        this.standardFunctionResolution = standardFunctionResolution;
        this.predicate = predicate;
        this.vectorSize = 1024;
        this.filter = filter;
        this.shardUuid = shardUuid;
        this.rowCount = rowCount;
        this.columnHandles = columnHandles;
        this.columnReaders = new CStoreColumnReader[columnHandles.size()];
        this.cursors = new VectorCursor[columnHandles.size()];
        this.columnHandleMap = new HashMap<>();
        columnHandles.forEach(columnHandle -> columnHandleMap.put(columnHandle.getColumnName(), columnHandle));

        setup();
    }

    public void setup()
    {
        Map<String, CStoreColumnReader> columnReaderMap = new HashMap<>();
        for (int i = 0; i < columnHandles.size(); i++) {
            columnReaders[i] = storageManager.getColumnReader(shardUuid, columnHandles.get(i).getColumnId());
            columnReaders[i].setup();
            columnReaderMap.put(columnHandles.get(i).getColumnName(), columnReaders[i]);
        }
        IndexFilterInterpreter indexFilterInterpreter = new IndexFilterInterpreter(this.typeManager,
                this.functionMetadataManager,
                this.standardFunctionResolution);
        IndexFilterInterpreter.Context interpreterContext = new IndexFilterInterpreter.Context()
        {
            @Override
            public CStoreColumnReader getColumnReader(String column)
            {
                return columnReaderMap.get(column);
            }

            @Override
            public BitmapColumnReader getBitmapReader(String column)
            {
                return storageManager.getBitmapReader(shardUuid, columnHandleMap.get(column).getColumnId());
            }
        };
        this.mask = indexFilterInterpreter.compute(filter, rowCount, vectorSize, interpreterContext);
        for (int i = 0; i < columnHandles.size(); i++) {
            cursors[i] = columnReaders[i].createVectorCursor(vectorSize);
            this.systemMemoryUsage += cursors[i].getSizeInBytes();
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getCompletedPositions()
    {
        return completedPositions;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return !mask.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            this.completedPositions = rowCount;
            return null;
        }
        Stopwatch stopwatch = Stopwatch.createStarted();

        SelectedPositions selection = mask.next();
        for (int i = 0; i < cursors.length; i++) {
            VectorCursor cursor = cursors[i];
            CStoreColumnReader columnReader = columnReaders[i];
            if (selection.isList()) {
                columnReader.read(selection.getPositions(), selection.getOffset(), selection.size(), cursor, 0);
            }
            else {
                columnReader.read(selection.getOffset(), selection.size(), cursor, 0);
            }
        }
        Block[] blocks = new Block[cursors.length];
        for (int i = 0; i < cursors.length; i++) {
            VectorCursor cursor = cursors[i];
            blocks[i] = cursor.toBlock(selection.size());
            this.completedBytes += cursor.getSizeInBytes();
        }

        if (selection.size() > 0) {
            if (selection.isList()) {
                this.completedPositions = selection.getPositions()[selection.size() - 1];
            }
            else {
                this.completedPositions = selection.getOffset() + selection.size();
            }
        }

        this.readTimeNanos += stopwatch.elapsed(TimeUnit.NANOSECONDS);
        return new Page(selection.size(), blocks);
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return systemMemoryUsage;
    }

    @Override
    public void close()
            throws IOException
    {
        for (CStoreColumnReader columnReader : columnReaders) {
            columnReader.close();
        }
        log.info("read cost %d ms", TimeUnit.NANOSECONDS.toMillis(readTimeNanos));
    }
}
