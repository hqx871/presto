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
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.stream.Collectors;

public class CStoreShardWalSink
        implements ShardSink
{
    private final URI uri;
    private File file;
    private final PagesSerde pagesSerde;
    private SliceOutput sliceOutput;
    private final ShardSink delegate;

    public CStoreShardWalSink(URI uri, PagesSerdeFactory pagesSerdeFactory, ShardSink delegate, boolean overwrite)
    {
        this.uri = uri;
        this.delegate = delegate;
        this.file = new File(uri);
        this.sliceOutput = openOutput(file, overwrite);
        this.pagesSerde = pagesSerdeFactory.createPagesSerde();
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
        delegate.reset();
        flush();
        URI uri = file.toURI();
        file.delete();
        file = new File(uri);
        this.sliceOutput = openOutput(file, true);
    }

    private SliceOutput openOutput(File file, boolean overwrite)
    {
        try {
            OpenOption openOption = overwrite ? StandardOpenOption.WRITE : StandardOpenOption.APPEND;
            DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(file.toPath(), openOption)));
            SliceOutput sliceOutput = new OutputStreamSliceOutput(output);
            if (overwrite) {
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
            }
            return sliceOutput;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void appendPage(Page page)
    {
        PagesSerdeUtil.writePages(pagesSerde, sliceOutput, page);
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

    public void flush()
    {
        try {
            sliceOutput.flush();
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
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

    public static ShardSink recoverFromUri(URI uri, long maxShardSize, PagesSerdeFactory pagesSerdeFactory)
    {
        File file = new File(uri);
        InputStream in;
        try {
            in = new BufferedInputStream(new FileInputStream(file));
        }
        catch (FileNotFoundException e) {
            throw new IllegalStateException(e);
        }
        SliceInput sliceInput = new InputStreamSliceInput(in);
        long uuidMostBits = sliceInput.readLong();
        long uuidLeastBits = sliceInput.readLong();
        UUID uuid = new UUID(uuidMostBits, uuidLeastBits);
        boolean unknownTable = sliceInput.readByte() == 0;
        OptionalLong tableId = unknownTable ? OptionalLong.empty() : OptionalLong.of(sliceInput.readLong());
        boolean unknownPartitionDay = sliceInput.readByte() == 0;
        OptionalInt day = unknownPartitionDay ? OptionalInt.empty() : OptionalInt.of(sliceInput.readInt());
        boolean unknownBucketNumber = sliceInput.readByte() == 0;
        OptionalInt bucketNumber = unknownBucketNumber ? OptionalInt.empty() : OptionalInt.of(sliceInput.readInt());
        int columnCount = sliceInput.readInt();
        JsonCodec<CStoreColumnHandle> codec = JsonCodec.jsonCodec(CStoreColumnHandle.class);
        List<CStoreColumnHandle> columnHandles = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            int size = sliceInput.readInt();
            byte[] bytes = new byte[size];
            sliceInput.read(bytes);
            CStoreColumnHandle columnHandle = codec.fromBytes(bytes);
            columnHandles.add(columnHandle);
        }
        PagesSerde pagesSerde = pagesSerdeFactory.createPagesSerde();
        Iterator<Page> iterator = PagesSerdeUtil.readPages(pagesSerde, sliceInput);
        List<Long> sortColumns = columnHandles.stream()
                .filter(columnHandle -> columnHandle.getSortOrdinal().isPresent())
                .map(CStoreColumnHandle::getColumnId)
                .collect(Collectors.toList());

        ShardSink delegate;
        if (sortColumns.isEmpty()) {
            delegate = new CStoreShardSimpleSink(uuid, maxShardSize, columnHandles, tableId, day, bucketNumber);
        }
        else {
            delegate = new CStoreShardSortSink(uuid, maxShardSize, columnHandles, sortColumns, tableId, day, bucketNumber);
        }
        while (iterator.hasNext()) {
            delegate.appendPage(iterator.next());
        }
        return new CStoreShardWalSink(uri, pagesSerdeFactory, delegate, false);
    }
}
