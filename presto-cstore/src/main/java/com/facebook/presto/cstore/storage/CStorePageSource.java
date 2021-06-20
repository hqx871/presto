package com.facebook.presto.cstore.storage;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.cstore.CStoreColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.base.Stopwatch;
import github.cstore.column.BitmapColumnReader;
import github.cstore.column.CStoreColumnReader;
import github.cstore.column.LongCursor;
import github.cstore.column.VectorCursor;
import github.cstore.filter.IndexFilterInterpreter;
import github.cstore.filter.SelectedPositions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
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

    private final Iterator<SelectedPositions> mask;
    private final List<CStoreColumnReader> columnReaders;
    private long completedBytes;
    private long completedPositions;
    private long readTimeNanos;
    private long systemMemoryUsage;
    private final int vectorSize;
    private final VectorCursor[] cursors;
    private final UUID shardUuid;
    private final int rowCount;
    private final List<CStoreColumnHandle> columnHandles;

    public CStorePageSource(List<CStoreColumnHandle> columnHandles, List<CStoreColumnReader> columnReaders,
            UUID shardUuid, int rowCount, Iterator<SelectedPositions> mask, int vectorSize)
    {
        this.mask = mask;
        this.vectorSize = vectorSize;
        this.shardUuid = shardUuid;
        this.rowCount = rowCount;
        this.columnHandles = columnHandles;
        this.columnReaders = columnReaders;
        this.cursors = new VectorCursor[columnHandles.size()];

        setup();
    }

    public void setup()
    {
        for (int i = 0; i < columnHandles.size(); i++) {
            cursors[i] = columnReaders.get(i).createVectorCursor(vectorSize);
            this.systemMemoryUsage += cursors[i].getSizeInBytes();
        }
    }

    public static CStorePageSource create(CStoreStorageManager storageManager,
            TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution,
            List<CStoreColumnHandle> columnHandles,
            UUID shardUuid,
            @Nullable RowExpression filter,
            int rowCount)
    {
        Map<String, CStoreColumnReader> columnReaderMap = new HashMap<>();
        Map<String, CStoreColumnHandle> columnHandleMap = new HashMap<>();
        int vectorSize = 1024;
        List<CStoreColumnReader> columnReaders = new ArrayList<>(columnHandles.size());
        for (int i = 0; i < columnHandles.size(); i++) {
            CStoreColumnHandle columnHandle = columnHandles.get(i);
            columnHandleMap.put(columnHandle.getColumnName(), columnHandle);
            if (columnHandle.isShardRowId()) {
                columnReaders.add(new ColumnRowIdReader(rowCount));
            }
            else {
                columnReaders.add(storageManager.getColumnReader(shardUuid, columnHandles.get(i).getColumnId()));
            }
            columnReaders.get(i).setup();
            columnReaderMap.put(columnHandles.get(i).getColumnName(), columnReaders.get(i));
        }
        IndexFilterInterpreter indexFilterInterpreter = new IndexFilterInterpreter(typeManager,
                functionMetadataManager,
                standardFunctionResolution);
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
        Iterator<SelectedPositions> mask = indexFilterInterpreter.compute(filter, rowCount, vectorSize, interpreterContext);
        return new CStorePageSource(columnHandles, columnReaders, shardUuid, rowCount, mask, vectorSize);
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
            CStoreColumnReader columnReader = columnReaders.get(i);
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

    public static final class ColumnRowIdReader
            implements CStoreColumnReader
    {
        private final int rowCount;

        public ColumnRowIdReader(int rowCount)
        {
            this.rowCount = rowCount;
        }

        @Override
        public void setup()
        {
        }

        @Override
        public int read(int offset, int size, VectorCursor dst, int dstOffset)
        {
            for (int i = 0; i < size; i++) {
                dst.writeLong(dstOffset + i, offset + i);
            }
            return size;
        }

        @Override
        public int read(int[] positions, int offset, int size, VectorCursor dst, int dstOffset)
        {
            for (int i = 0; i < size; i++) {
                dst.writeLong(dstOffset + i, positions[offset + i]);
            }
            return size;
        }

        @Override
        public int getRowCount()
        {
            return rowCount;
        }

        @Override
        public VectorCursor createVectorCursor(int size)
        {
            return new LongCursor(new long[size]);
        }

        @Override
        public void close()
        {
        }
    }
}
