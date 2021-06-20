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
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorPageSink;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class CStoreStashPageSink
        implements ConnectorPageSink
{
    private final ConnectorPageSink delegate;
    private final PageBuilder pageBuilder;

    public CStoreStashPageSink(
            List<CStoreColumnHandle> columnHandles,
            ConnectorPageSink delegate)
    {
        List<Type> columnTypes = columnHandles.stream().map(CStoreColumnHandle::getColumnType).collect(Collectors.toList());
        this.delegate = delegate;
        this.pageBuilder = new PageBuilder(columnTypes);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (page.getPositionCount() == 0) {
            return NOT_BLOCKED;
        }
        for (int position = 0; position < page.getPositionCount(); position++) {
            pageBuilder.declarePosition();
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(channel);
                pageBuilder.getType(channel).appendTo(block, position, blockBuilder);
            }

            if (pageBuilder.isFull()) {
                delegate.appendPage(pageBuilder.build());
                pageBuilder.reset();
            }
        }
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
        if (pageBuilder.getPositionCount() > 0) {
            delegate.appendPage(pageBuilder.build());
            pageBuilder.reset();
        }
    }
}
