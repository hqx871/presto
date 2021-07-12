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
import com.facebook.presto.cstore.storage.organization.TemporalFunction;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorPageSink;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.facebook.airlift.concurrent.MoreFutures.allAsList;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class CStorePageBucketSink
        implements ConnectorPageSink
{
    private final List<Type> columnTypes;
    private final OptionalInt bucketCount;
    private final int[] bucketFields;
    private final OptionalInt temporalColumnIndex;
    private final Optional<Type> temporalColumnType;
    private final TemporalFunction temporalFunction;

    private final Optional<BucketFunction> bucketFunction;

    private final Long2ObjectMap<ConnectorPageSink> bucketSinks = new Long2ObjectOpenHashMap<>();
    private final CStorePageSinkFactory delegateFactory;
    @Deprecated
    private final PageBuilder pageBuilder;

    public CStorePageBucketSink(
            List<CStoreColumnHandle> columnHandles,
            TemporalFunction temporalFunction,
            OptionalInt bucketCount,
            List<Long> bucketColumnIds,
            Optional<CStoreColumnHandle> temporalColumnHandle,
            CStorePageSinkFactory delegateFactory)
    {
        this.temporalFunction = requireNonNull(temporalFunction, "temporalFunction is null");
        List<Long> columnIds = columnHandles.stream().map(CStoreColumnHandle::getColumnId).collect(toList());
        this.columnTypes = columnHandles.stream().map(CStoreColumnHandle::getColumnType).collect(toList());

        this.bucketCount = bucketCount;
        this.bucketFields = bucketColumnIds.stream().mapToInt(columnIds::indexOf).toArray();
        this.delegateFactory = delegateFactory;

        if (temporalColumnHandle.isPresent() && columnIds.contains(temporalColumnHandle.get().getColumnId())) {
            temporalColumnIndex = OptionalInt.of(columnIds.indexOf(temporalColumnHandle.get().getColumnId()));
            temporalColumnType = Optional.of(columnTypes.get(temporalColumnIndex.getAsInt()));
            checkArgument(temporalColumnType.get() == DATE || temporalColumnType.get() == TIMESTAMP,
                    "temporalColumnType can only be DATE or TIMESTAMP");
        }
        else {
            temporalColumnIndex = OptionalInt.empty();
            temporalColumnType = Optional.empty();
        }
        List<Type> bucketTypes = Arrays.stream(bucketFields)
                .mapToObj(columnTypes::get)
                .collect(toList());
        this.pageBuilder = new PageBuilder(columnTypes);
        this.bucketFunction = bucketCount.isPresent() ? Optional.of(new CStoreBucketFunction(bucketCount.getAsInt(), bucketTypes)) : Optional.empty();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (page.getPositionCount() == 0) {
            return NOT_BLOCKED;
        }

        Block temporalBlock = temporalColumnIndex.isPresent() ? page.getBlock(temporalColumnIndex.getAsInt()) : null;
        Page bucketArgs = bucketFunction.isPresent() ? getBucketArgsPage(page) : null;

        for (int position = 0; position < page.getPositionCount(); position++) {
            int bucket = bucketFunction.isPresent() ? bucketFunction.get().getBucket(bucketArgs, position) : 0;
            int day = temporalColumnType.isPresent() ? temporalFunction.getDay(temporalColumnType.get(), temporalBlock, position) : 0;

            long partition = (((long) bucket) << 32) | (day & 0xFFFF_FFFFL);
            ConnectorPageSink bucketSink = bucketSinks.get(partition);
            if (bucketSink == null) {
                bucketSink = delegateFactory.create(OptionalInt.of(day), OptionalInt.of(bucket));
                bucketSinks.put(partition, bucketSink);
            }
            bucketSink.appendPage(page.getRegion(position, 1));
            //appendPosition(store, page, position);
        }

        return NOT_BLOCKED;
    }

    private void appendPosition(ConnectorPageSink pageSink, Page page, int position)
    {
        pageBuilder.declarePosition();
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            Block block = page.getBlock(channel);
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(channel);
            pageBuilder.getType(channel).appendTo(block, position, blockBuilder);
        }
        pageBuilder.reset();
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return allAsList(bucketSinks.values().stream()
                .map(ConnectorPageSink::finish)
                .collect(Collectors.toList()))
                .thenApply(list -> list.stream().flatMap(Collection::stream).collect(toList()));
    }

    @Override
    public void abort()
    {
        RuntimeException error = new RuntimeException("Exception during rollback");
        for (ConnectorPageSink pageBuffer : bucketSinks.values()) {
            try {
                pageBuffer.abort();
            }
            catch (Throwable t) {
                // Self-suppression not permitted
                if (error != t) {
                    error.addSuppressed(t);
                }
            }
        }
        if (error.getSuppressed().length > 0) {
            throw error;
        }
    }

    private Page getBucketArgsPage(Page page)
    {
        Block[] blocks = new Block[bucketFields.length];
        for (int i = 0; i < bucketFields.length; i++) {
            blocks[i] = page.getBlock(bucketFields[i]);
        }
        return new Page(page.getPositionCount(), blocks);
    }
}
