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
import org.apache.cstore.SelectedPositions;
import org.apache.cstore.column.CStoreColumnReader;
import org.apache.cstore.column.CStoreColumnReaderFactory;
import org.apache.cstore.filter.IndexFilterInterpreter;
import org.apache.cstore.manage.CStoreDatabase;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;

public class CStorePageSource
        implements ConnectorPageSource
{
    private final CStoreDatabase database;
    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;
    private final ConnectorSession session;
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
            ConnectorSession session, CStoreSplit split,
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

        IndexFilterInterpreter indexFilterInterpreter = new IndexFilterInterpreter(this.typeManager,
                this.functionMetadataManager,
                this.standardFunctionResolution,
                this.session,
                this.database,
                split);
        this.mask = indexFilterInterpreter.compute(filter, split.getRowCount(), vectorSize);
        this.columnReaders = new CStoreColumnReader[columnHandles.length];
        CStoreColumnReaderFactory columnReaderFactory = new CStoreColumnReaderFactory();
        for (int i = 0; i < columnHandles.length; i++) {
            columnReaders[i] = columnReaderFactory.open(split, columnHandles[i]);
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
            return null;
        }
        BlockBuilder[] blockBuilders = new BlockBuilder[columns.length];
        for (int i = 0; i < columns.length; i++) {
            CStoreColumnHandle columnHandle = columns[i];
            Type type = columnHandle.getColumnType();
            blockBuilders[i] = type.createBlockBuilder(null, vectorSize);
        }

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
            blockBuilder.closeEntry();
            blocks[i] = blockBuilder.build();
        }
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
