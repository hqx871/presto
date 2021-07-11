package com.facebook.presto.cstore.storage;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.cstore.CStoreColumnHandle;
import com.facebook.presto.cstore.metadata.ShardDelta;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.google.common.collect.ImmutableList;
import github.cstore.bitmap.Bitmap;
import github.cstore.bitmap.BitmapIterator;
import github.cstore.bitmap.RoaringBitmapAdapter;
import github.cstore.column.CStoreColumnReader;
import github.cstore.filter.SelectedPositions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;

public class CStoreUpdatablePageSource
        implements UpdatablePageSource
{
    private static final Logger log = Logger.get(CStoreUpdatablePageSource.class);

    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;
    private final CStoreStorageManager storageManager;
    private final UUID shardUuid;
    private final List<CStoreColumnHandle> columnHandles;
    //private final Map<String, CStoreColumnHandle> columnHandleMap;
    private final MutableRoaringBitmap mask;
    private final OptionalInt bucketNumber;
    private final long transactionId;
    private final ConnectorPageSource source;
    private final int rowCount;

    public CStoreUpdatablePageSource(CStoreStorageManager storageManager,
            TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution,
            List<CStoreColumnHandle> columnHandles,
            UUID shardUuid,
            OptionalInt bucketNumber,
            long transactionId,
            ConnectorPageSource source,
            int rowCount)
    {
        this.storageManager = storageManager;
        this.typeManager = typeManager;
        this.functionMetadataManager = functionMetadataManager;
        this.standardFunctionResolution = standardFunctionResolution;
        this.bucketNumber = bucketNumber;
        this.transactionId = transactionId;
        this.shardUuid = shardUuid;
        this.columnHandles = columnHandles;
        this.rowCount = rowCount;
        //this.columnHandleMap = new HashMap<>();
        //columnHandles.forEach(columnHandle -> columnHandleMap.put(columnHandle.getColumnName(), columnHandle));
        this.mask = new MutableRoaringBitmap();
        this.source = source;
    }

    @Override
    public long getCompletedBytes()
    {
        return source.getCompletedBytes();
    }

    @Override
    public long getCompletedPositions()
    {
        return source.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return source.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return source.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        return source.getNextPage();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return source.getSystemMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        source.close();
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        for (int i = 0; i < rowIds.getPositionCount(); i++) {
            long rowId = BIGINT.getLong(rowIds, i);
            mask.add(toIntExact(rowId));
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return delete();
    }

    private CompletableFuture<Collection<Slice>> delete()
    {
        List<CStoreColumnHandle> columnHandles = this.columnHandles.stream()
                .filter(columnHandle -> !CStoreColumnHandle.isHiddenColumn(columnHandle.getColumnId()))
                .collect(Collectors.toList());
        mask.flip(0L, rowCount);
        Bitmap bitmap = new RoaringBitmapAdapter(rowCount, mask);
        final int vectorSize = 1024;
        Iterator<SelectedPositions> iterator = new Iterator<SelectedPositions>()
        {
            final BitmapIterator delegate = bitmap.iterator();
            final int[] positions = new int[vectorSize];

            @Override
            public boolean hasNext()
            {
                return delegate.hasNext();
            }

            @Override
            public SelectedPositions next()
            {
                int count = delegate.next(positions);
                return SelectedPositions.positionsList(positions, 0, count);
            }
        };
        List<CStoreColumnReader> columnReaders = new ArrayList<>(columnHandles.size());
        for (int i = 0; i < columnHandles.size(); i++) {
            CStoreColumnHandle columnHandle = columnHandles.get(i);
            //columnHandleMap.put(columnHandle.getColumnName(), columnHandle);
            if (columnHandle.isShardRowId()) {
                columnReaders.add(new CStorePageSource.ColumnRowIdReader(rowCount));
            }
            else {
                columnReaders.add(storageManager.getColumnReader(shardUuid, columnHandles.get(i).getColumnId()));
            }
            columnReaders.get(i).setup();
        }
        ConnectorPageSource source = new CStorePageSource(columnReaders, rowCount, iterator, vectorSize);
        StoragePageSink sink = storageManager.createStoragePageSink(transactionId, bucketNumber, columnHandles, false);

        for (Page page = source.getNextPage(); page != null; page = source.getNextPage()) {
            sink.appendPages(ImmutableList.of(page));
            if (sink.isFull()) {
                sink.flush();
            }
        }
        sink.flush();
        return sink.commit().thenApply(shards -> {
            ShardDelta shardDelta = new ShardDelta(ImmutableList.of(shardUuid), shards);
            return ImmutableList.of(Slices.wrappedBuffer(ShardDelta.JSON_CODEC.toBytes(shardDelta)));
        });
    }
}
