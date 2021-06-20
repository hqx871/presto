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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cstore.CStoreColumnHandle;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;

public class MemoryPageBuffer
{
    private final long maxMemoryBytes;
    private final List<Page> pages = new ArrayList<>();

    private long usedMemoryBytes;
    private long rowCount;
    private final PageBuilder pageBuilder;
    private final UUID uuid;
    private final List<CStoreColumnHandle> columnHandles;

    public MemoryPageBuffer(
            UUID uuid,
            long maxMemoryBytes,
            List<Type> columnTypes,
            List<CStoreColumnHandle> columnHandles)
    {
        this.columnHandles = columnHandles;
        checkArgument(maxMemoryBytes > 0, "maxMemoryBytes must be positive");
        this.maxMemoryBytes = maxMemoryBytes;
        this.pageBuilder = new PageBuilder(columnTypes);
        this.uuid = uuid;
    }

    public long getUsedMemoryBytes()
    {
        return usedMemoryBytes;
    }

    public List<Page> getPages()
    {
        return pages;
    }

    public void reset()
    {
        flush();
    }

    public void appendPage(Page page)
    {
        flushIfNecessary(page.getPositionCount());
        pages.add(page);
        usedMemoryBytes += page.getSizeInBytes();
        rowCount += page.getPositionCount();
    }

    public void appendPages(List<Page> inputPages, int[] pageIndexes, int[] positionIndexes)
    {
        for (int i = 0; i < pageIndexes.length; i++) {
            pageBuilder.declarePosition();
            Page page = inputPages.get(pageIndexes[i]);
            int position = positionIndexes[i];
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(channel);
                pageBuilder.getType(channel).appendTo(block, position, blockBuilder);
            }

            if (pageBuilder.isFull()) {
                appendPage(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        if (pageBuilder.getPositionCount() > 0) {
            appendPage(pageBuilder.build());
            pageBuilder.reset();
        }
    }

    private void flush()
    {
        if (pages.isEmpty()) {
            return;
        }
        pages.clear();
        rowCount = 0;
        usedMemoryBytes = 0;
    }

    public long getRowCount()
    {
        return rowCount;
    }

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

    public List<CStoreColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    public boolean canAddRows(int rowsToAdd)
    {
        return (usedMemoryBytes < maxMemoryBytes) &&
                ((rowCount + rowsToAdd) < Integer.MAX_VALUE);
    }
}
