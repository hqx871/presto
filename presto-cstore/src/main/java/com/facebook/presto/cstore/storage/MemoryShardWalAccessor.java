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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.Page;
import com.facebook.presto.cstore.CStoreColumnHandle;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.PagesSerdeUtil;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceOutput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.UUID;

public class MemoryShardWalAccessor
        implements MemoryShardAccessor
{
    private final long transaction;
    private final UUID shardUuid;
    private final WriteAheadLogAppender walAppender;
    private final PagesSerde pagesSerde;
    private final MemoryShardAccessor delegate;

    public MemoryShardWalAccessor(UUID shardUuid, WriteAheadLogAppender walAppender, PagesSerdeFactory pagesSerdeFactory,
            MemoryShardAccessor delegate, long transaction)
    {
        this.walAppender = walAppender;
        this.delegate = delegate;
        this.pagesSerde = pagesSerdeFactory.createPagesSerde();
        this.shardUuid = shardUuid;
        this.transaction = transaction;

        initShardMetadata();
    }

    @Override
    public long getUsedMemoryBytes()
    {
        return delegate.getUsedMemoryBytes();
    }

    @Override
    public List<Page> getPages()
    {
        return delegate.getPages();
    }

    @Override
    public void reset()
    {
    }

    private void initShardMetadata()
    {
        try {
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            SliceOutput sliceOutput = new OutputStreamSliceOutput(byteArray);
            sliceOutput.writeLong(transaction);
            sliceOutput.writeLong(delegate.getUuid().getMostSignificantBits());
            sliceOutput.writeLong(delegate.getUuid().getLeastSignificantBits());
            if (getTableId().isPresent()) {
                sliceOutput.writeByte(1);
                sliceOutput.writeLong(getTableId().getAsLong());
            }
            else {
                sliceOutput.writeByte(0);
            }
            if (getPartitionDay().isPresent()) {
                sliceOutput.writeByte(1);
                sliceOutput.writeInt(getPartitionDay().getAsInt());
            }
            else {
                sliceOutput.writeByte(0);
            }
            if (getBucketNumber().isPresent()) {
                sliceOutput.writeByte(1);
                sliceOutput.writeInt(getBucketNumber().getAsInt());
            }
            else {
                sliceOutput.writeByte(0);
            }
            sliceOutput.writeInt(getColumnHandles().size());
            JsonCodec<CStoreColumnHandle> codec = JsonCodec.jsonCodec(CStoreColumnHandle.class);
            for (CStoreColumnHandle columnHandle : getColumnHandles()) {
                byte[] bytes = codec.toBytes(columnHandle);
                sliceOutput.writeInt(bytes.length);
                sliceOutput.write(bytes);
            }
            byte[] bytes = byteArray.toByteArray();
            walAppender.append(transaction, shardUuid, ActionType.CREATE, bytes);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void appendPage(Page page)
    {
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        SliceOutput sliceOutput = new OutputStreamSliceOutput(byteArray);
        PagesSerdeUtil.writePages(pagesSerde, sliceOutput, page);
        byte[] bytes = byteArray.toByteArray();
        walAppender.append(transaction, shardUuid, ActionType.APPEND, bytes);
        delegate.appendPage(page);
    }

    @Override
    public void appendPages(List<Page> inputPages, int[] pageIndexes, int[] positionIndexes)
    {
        for (int i = 0; i < pageIndexes.length; i++) {
            Page page = inputPages.get(pageIndexes[i]);
            int position = positionIndexes[i];
            appendPage(page.getSingleValuePage(position));
        }
    }

    @Override
    public long getRowCount()
    {
        return delegate.getRowCount();
    }

    @Override
    public UUID getUuid()
    {
        return delegate.getUuid();
    }

    @Override
    public long getTransactionId()
    {
        return transaction;
    }

    @Override
    public List<CStoreColumnHandle> getColumnHandles()
    {
        return delegate.getColumnHandles();
    }

    @Override
    public boolean canAddRows(int rowsToAdd)
    {
        return delegate.canAddRows(rowsToAdd);
    }

    @Override
    public void commit()
    {
        walAppender.append(transaction, shardUuid, ActionType.COMMIT, new byte[0]);
        delegate.commit();
    }

    @Override
    public void rollback()
    {
        walAppender.append(transaction, shardUuid, ActionType.ROLLBACK, new byte[0]);
        delegate.rollback();
    }

    @Override
    public OptionalLong getTableId()
    {
        return delegate.getTableId();
    }

    @Override
    public OptionalInt getPartitionDay()
    {
        return delegate.getPartitionDay();
    }

    @Override
    public OptionalInt getBucketNumber()
    {
        return delegate.getBucketNumber();
    }
}
