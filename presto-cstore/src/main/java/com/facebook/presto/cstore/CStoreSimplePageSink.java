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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.Page;
import com.facebook.presto.cstore.metadata.ShardInfo;
import com.facebook.presto.cstore.storage.StoragePageSink;
import com.facebook.presto.spi.ConnectorPageSink;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static java.util.stream.Collectors.toList;

public class CStoreSimplePageSink
        implements ConnectorPageSink
{
    private static final JsonCodec<ShardInfo> SHARD_INFO_CODEC = jsonCodec(ShardInfo.class);
    protected final StoragePageSink storagePageSink;

    public CStoreSimplePageSink(
            StoragePageSink storagePageSink)
    {
        this.storagePageSink = storagePageSink;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (page.getPositionCount() == 0) {
            return NOT_BLOCKED;
        }

        storagePageSink.appendPages(ImmutableList.of(page));
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        //storagePageSink.flush();
        CompletableFuture<List<ShardInfo>> futureShards = storagePageSink.commit();
        return futureShards.thenApply(shards -> shards.stream()
                .map(shard -> Slices.wrappedBuffer(SHARD_INFO_CODEC.toJsonBytes(shard)))
                .collect(toList()));
    }

    @Override
    public void abort()
    {
        RuntimeException error = new RuntimeException("Exception during rollback");
        try {
            storagePageSink.rollback();
        }
        catch (Throwable t) {
            // Self-suppression not permitted
            if (error != t) {
                error.addSuppressed(t);
            }
        }
        if (error.getSuppressed().length > 0) {
            throw error;
        }
    }
}
