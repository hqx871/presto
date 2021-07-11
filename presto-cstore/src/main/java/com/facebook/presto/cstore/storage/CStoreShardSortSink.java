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
package com.facebook.presto.cstore.storage;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cstore.CStoreColumnHandle;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class CStoreShardSortSink
        implements ShardSink
{
    private final long maxMemoryBytes;
    private final SortedMap<Object, List<Row>> rows;

    private long usedMemoryBytes;
    private long rowCount;
    private final UUID uuid;
    private final List<CStoreColumnHandle> columnHandles;
    private final OptionalLong tableId;
    private final OptionalInt partitionDay;
    private final OptionalInt bucketNumber;
    private final List<Type> columnTypes;
    private final List<Integer> sortColumnOrdinals;

    public CStoreShardSortSink(
            UUID uuid,
            long maxMemoryBytes,
            List<CStoreColumnHandle> columnHandles,
            List<Long> sortColumns,
            OptionalLong tableId,
            OptionalInt partitionDay,
            OptionalInt bucketNumber)
    {
        this.columnHandles = columnHandles;
        this.tableId = tableId;
        this.partitionDay = partitionDay;
        this.bucketNumber = bucketNumber;
        checkArgument(maxMemoryBytes > 0, "maxMemoryBytes must be positive");
        this.maxMemoryBytes = maxMemoryBytes;
        this.columnTypes = columnHandles.stream().map(CStoreColumnHandle::getColumnType).collect(Collectors.toList());

        this.sortColumnOrdinals = new ArrayList<>();
        for (int i = 0; i < sortColumns.size(); i++) {
            Long sortColumnId = sortColumns.get(i);
            for (CStoreColumnHandle columnHandle : columnHandles) {
                if (sortColumnId.equals(columnHandle.getColumnId())) {
                    sortColumnOrdinals.add(i);
                }
            }
        }
        this.uuid = uuid;
        this.rows = new ConcurrentSkipListMap<>();
    }

    @Override
    public long getUsedMemoryBytes()
    {
        return usedMemoryBytes;
    }

    @Override
    public void reset()
    {
        flush();
    }

    @Override
    public void appendPage(Page page)
    {
        flushIfNecessary(page.getPositionCount());
        for (int i = 0; i < page.getPositionCount(); i++) {
            appendPage(page, i);
        }
        //usedMemoryBytes += page.getSizeInBytes();
        //rowCount += page.getPositionCount();
    }

    private void appendPage(Page page, int position)
    {
        Row row = Row.extractRow(page, position, columnTypes);
        List<Object> key = new ArrayList<>(sortColumnOrdinals.size());
        for (int j = 0; j < sortColumnOrdinals.size(); j++) {
            key.add(row.getColumns().get(j));
        }
        rows.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        usedMemoryBytes += row.getSizeInBytes();
        rowCount++;
    }

    @Override
    public void appendPages(List<Page> inputPages, int[] pageIndexes, int[] positionIndexes)
    {
        checkArgument(pageIndexes.length == positionIndexes.length, "pageIndexes and positionIndexes do not match");
        for (int i = 0; i < pageIndexes.length; i++) {
            Page page = inputPages.get(pageIndexes[i]);
            int position = positionIndexes[i];
            appendPage(page, position);
        }
    }

    private void flush()
    {
        rowCount = 0;
        usedMemoryBytes = 0;
    }

    @Override
    public long getRowCount()
    {
        return rowCount;
    }

    @Override
    public UUID getUuid()
    {
        return uuid;
    }

    private void flushIfNecessary(int rowsToAdd)
    {
        if (!canAddRows(rowsToAdd)) {
            flush();
        }
    }

    @Override
    public List<CStoreColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @Override
    public boolean canAddRows(int rowsToAdd)
    {
        return (usedMemoryBytes < maxMemoryBytes) &&
                ((rowCount + rowsToAdd) < Integer.MAX_VALUE);
    }

    @Override
    public OptionalLong getTableId()
    {
        return tableId;
    }

    @Override
    public OptionalInt getPartitionDay()
    {
        return partitionDay;
    }

    @Override
    public OptionalInt getBucketNumber()
    {
        return bucketNumber;
    }

    @Override
    public List<Page> getPages()
    {
        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        List<Page> pages = new ArrayList<>();
        for (List<Row> rowBatch : rows.values()) {
            for (Row row : rowBatch) {
                row.appendTo(pageBuilder, columnTypes);
                if (pageBuilder.isFull()) {
                    pages.add(pageBuilder.build());
                    pageBuilder.reset();
                }
            }
        }
        if (pageBuilder.getPositionCount() > 0) {
            pages.add(pageBuilder.build());
            pageBuilder.reset();
        }
        return pages;
    }
}
