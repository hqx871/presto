/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.cstore;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cstore.storage.MemoryPageBuffer;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.PageSorter;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class CStoreSortPageSink
        implements ConnectorPageSink
{
    protected final PageSorter pageSorter;
    protected final List<Type> columnTypes;
    protected final List<CStoreColumnHandle> columnHandles;
    protected final List<Integer> sortFields;
    protected final List<SortOrder> sortOrders;

    private final ConnectorPageSink delegate;
    private final MemoryPageBuffer pageBuffer;

    public CStoreSortPageSink(
            PageSorter pageSorter,
            List<CStoreColumnHandle> columnHandles,
            List<Long> sortColumnIds,
            List<SortOrder> sortOrders,
            long maxBufferSize,
            ConnectorPageSink delegate)
    {
        this.columnHandles = columnHandles;
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.delegate = delegate;
        List<Long> columnIds = columnHandles.stream().map(CStoreColumnHandle::getColumnId).collect(toList());
        this.columnTypes = columnHandles.stream().map(CStoreColumnHandle::getColumnType).collect(toList());
        this.sortFields = ImmutableList.copyOf(sortColumnIds.stream().map(columnIds::indexOf).collect(toList()));
        this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
        this.pageBuffer = new MemoryPageBuffer(UUID.randomUUID(), maxBufferSize, columnTypes, columnHandles);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (page.getPositionCount() == 0) {
            return NOT_BLOCKED;
        }
        if (!pageBuffer.canAddRows(page.getPositionCount())) {
            flush();
        }
        pageBuffer.appendPage(page);
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        flush();
        return delegate.finish();
    }

    @Override
    public void abort()
    {
        RuntimeException error = new RuntimeException("Exception during rollback");
        delegate.abort();
        if (error.getSuppressed().length > 0) {
            throw error;
        }
    }

    private void flush()
    {
        if (pageBuffer.getRowCount() > 0) {
            sortAndFlush(pageBuffer.getPages(), toIntExact(pageBuffer.getRowCount()));
        }
        pageBuffer.reset();
    }

    private void sortAndFlush(List<Page> pages, int rowCount)
    {
        long[] addresses = pageSorter.sort(columnTypes, pages, sortFields, sortOrders, toIntExact(rowCount));

        int[] pageIndex = new int[addresses.length];
        int[] positionIndex = new int[addresses.length];
        for (int i = 0; i < addresses.length; i++) {
            pageIndex[i] = pageSorter.decodePageIndex(addresses[i]);
            positionIndex[i] = pageSorter.decodePositionIndex(addresses[i]);
        }
        pageBuffer.appendPages(pages, pageIndex, positionIndex);
    }
}
