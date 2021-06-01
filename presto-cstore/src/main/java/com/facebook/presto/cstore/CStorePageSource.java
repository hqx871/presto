package com.facebook.presto.cstore;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.base.Stopwatch;
import org.apache.cstore.SelectedPositions;
import org.apache.cstore.column.BitmapColumnReader;
import org.apache.cstore.column.CStoreColumnReader;
import org.apache.cstore.column.CStoreColumnReaderFactory;
import org.apache.cstore.column.DictionaryReader;
import org.apache.cstore.dictionary.DictionaryBlockBuilder;
import org.apache.cstore.filter.IndexFilterInterpreter;
import org.apache.cstore.manage.CStoreDatabase;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CStorePageSource
        implements ConnectorPageSource
{
    private final CStoreDatabase database;
    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;
    private final ConnectorSession session;
    private final CStoreSplit split;
    private final CStoreColumnHandle[] columns;
    private final Iterator<SelectedPositions> mask;
    private final CStoreColumnReader[] columnReaders;
    private long completedBytes;
    private long completedPositions;
    private long readTimeNanos;
    private long systemMemoryUsage;
    private final int vectorSize;

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
        CStoreColumnReaderFactory columnReaderFactory = new CStoreColumnReaderFactory();
        Map<String, CStoreColumnReader> columnReaderMap = new HashMap<>();
        for (int i = 0; i < columnHandles.length; i++) {
            columnReaders[i] = columnReaderFactory.open(split, columnHandles[i]);
            columnReaderMap.put(columnHandles[i].getColumnName(), columnReaders[i]);
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
        //TableMeta tableMeta = database.getTableMeta(split.getSchema(), split.getTable());
        Stopwatch stopwatch = Stopwatch.createStarted();
        BlockBuilder[] blockBuilders = new BlockBuilder[columns.length];
        long newSystemMemoryUsage = 0;
        for (int i = 0; i < columns.length; i++) {
            CStoreColumnHandle columnHandle = columns[i];
            Type type = columnHandle.getColumnType();
            //ColumnMeta columnMeta = tableMeta.getColumn(columnHandle.getColumnName());
            if (columnReaders[i] instanceof DictionaryReader) {
                //todo only support string dictionary encode now. support long, double etc.
                DictionaryReader columnReader = (DictionaryReader) columnReaders[i];
                blockBuilders[i] = new DictionaryBlockBuilder(columnReader.getDictionaryValue(), new int[vectorSize], null);
            }
            else {
                blockBuilders[i] = type.createBlockBuilder(null, vectorSize);
            }
            newSystemMemoryUsage += blockBuilders[i].getRetainedSizeInBytes() + blockBuilders[i].getLogicalSizeInBytes();
            this.completedBytes += blockBuilders[i].getLogicalSizeInBytes();
        }
        this.systemMemoryUsage = newSystemMemoryUsage;

        SelectedPositions selection = mask.next();
        for (int i = 0; i < blockBuilders.length; i++) {
            BlockBuilder blockBuilder = blockBuilders[i];
            CStoreColumnReader columnReader = columnReaders[i];
            if (selection.isList()) {
                columnReader.read(selection.getPositions(), selection.getOffset(), selection.size(), blockBuilder);
            }
            else {
                columnReader.read(selection.getOffset(), selection.size(), blockBuilder);
            }
        }
        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blockBuilders.length; i++) {
            BlockBuilder blockBuilder = blockBuilders[i];
            //blockBuilder.closeEntry();
            blocks[i] = blockBuilder.build();
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
    }
}
