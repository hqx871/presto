package org.apache.cstore;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cstore.CStoreColumnHandle;
import com.facebook.presto.cstore.CStoreSplit;
import com.facebook.presto.spi.relation.RowExpression;
import org.apache.cstore.column.CStoreColumnReader;
import org.apache.cstore.column.CStoreColumnReaderFactory;
import org.apache.cstore.filter.IndexFilterInterpreter;

import javax.annotation.Nullable;

import java.util.Iterator;

public class CStoreReader
{
    private final CStoreColumnHandle[] columns;
    private final int vectorSize;
    private final Iterator<SelectedPositions> mask;
    private final CStoreColumnReader[] columnReaders;

    public CStoreReader(CStoreSplit split, CStoreColumnHandle[] columns, @Nullable RowExpression filter, int vectorSize)
    {
        this.columns = columns;
        this.vectorSize = vectorSize;
        this.mask = new IndexFilterInterpreter().compute(filter, split.getRowCount(), vectorSize);
        this.columnReaders = new CStoreColumnReader[columns.length];
        CStoreColumnReaderFactory columnReaderFactory = new CStoreColumnReaderFactory();
        for (int i = 0; i < columns.length; i++) {
            columnReaders[i] = columnReaderFactory.open(split, columns[i]);
        }
    }

    public Page getNextPage()
    {
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
                columnReader.read(selection.getPositions(), 0, selection.size(), blockBuilder);
            }
            else {
                columnReader.read(selection.getOffset(), selection.size(), blockBuilder);
            }
        }
        return new Page(blockBuilders);
    }

    public boolean isFinished()
    {
        return !mask.hasNext();
    }

    public void close()
    {
        for (CStoreColumnReader columnReader : columnReaders) {
            columnReader.close();
        }
    }
}
