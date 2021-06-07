package com.facebook.presto.cstore;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.base.Stopwatch;
import org.apache.cstore.CStoreDatabase;
import org.apache.cstore.column.BitmapColumnReader;
import org.apache.cstore.column.CStoreColumnReader;
import org.apache.cstore.column.VectorCursor;
import org.apache.cstore.filter.IndexFilterInterpreter;
import org.apache.cstore.filter.SelectedPositions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CStorePageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(CStorePageSource.class);

    private final CStoreDatabase database;
    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;
    private final ConnectorSession session;
    private final CStoreSplit split;
    private final CStoreColumnHandle[] columns;
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

    public CStorePageSource(CStoreDatabase database,
            TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution,
            ConnectorSession session,
            CStoreSplit split,
            CStoreColumnHandle[] columnHandles,
            @Nullable RowExpression filter)
    {
        this.database = database;
        this.typeManager = typeManager;
        this.functionMetadataManager = functionMetadataManager;
        this.standardFunctionResolution = standardFunctionResolution;
        this.session = session;
        this.vectorSize = 1024;
        this.columns = columnHandles;
        this.split = split;
        this.columnReaders = new CStoreColumnReader[columnHandles.length];
        this.filter = filter;
        this.cursors = new VectorCursor[columns.length];

        setup();
    }

    public void setup()
    {
        Map<String, CStoreColumnReader> columnReaderMap = new HashMap<>();
        for (int i = 0; i < columns.length; i++) {
            columnReaders[i] = database.getColumnReader(split.getSchema(), split.getTable(), columns[i].getColumnName());
            columnReaderMap.put(columns[i].getColumnName(), columnReaders[i]);
        }
        IndexFilterInterpreter indexFilterInterpreter = new IndexFilterInterpreter(this.typeManager,
                this.functionMetadataManager,
                this.standardFunctionResolution,
                this.session,
                this.database,
                split);
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
                return database.getBitmapReader(split.getSchema(), split.getTable(), column);
            }
        };
        this.mask = indexFilterInterpreter.compute(filter, split.getRowCount(), vectorSize, interpreterContext);
        for (int i = 0; i < columns.length; i++) {
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
            this.completedPositions = split.getRowCount();
            return null;
        }
        Stopwatch stopwatch = Stopwatch.createStarted();

        SelectedPositions selection = mask.next();
        for (int i = 0; i < cursors.length; i++) {
            VectorCursor cursor = cursors[i];
            CStoreColumnReader columnReader = columnReaders[i];
            if (selection.isList()) {
                columnReader.read(selection.getPositions(), selection.getOffset(), selection.size(), cursor);
            }
            else {
                columnReader.read(selection.getOffset(), selection.size(), cursor);
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
