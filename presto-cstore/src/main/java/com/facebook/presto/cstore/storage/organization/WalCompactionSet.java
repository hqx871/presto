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
package com.facebook.presto.cstore.storage.organization;

import java.util.List;
import java.util.OptionalInt;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class WalCompactionSet
{
    private final long tableId;
    private final List<UUID> shardUuids;
    private final OptionalInt bucketNumber;
    private final int priority;
    private final UUID walUuid;
    private final long walBaseOffset;
    private final long transactionEnd;

    public WalCompactionSet(long tableId, OptionalInt bucketNumber, List<UUID> shardUuids, int priority,
            UUID walUuid, long walBaseOffset, long transactionEnd)
    {
        this.tableId = tableId;
        this.shardUuids = shardUuids;
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.priority = priority;
        this.walUuid = walUuid;
        this.walBaseOffset = walBaseOffset;
        this.transactionEnd = transactionEnd;
    }

    public long getTableId()
    {
        return tableId;
    }

    public List<UUID> getShardUuids()
    {
        return shardUuids;
    }

    public UUID getWalUuid()
    {
        return walUuid;
    }

    public OptionalInt getBucketNumber()
    {
        return bucketNumber;
    }

    public int getPriority()
    {
        return priority;
    }

    public long getTransactionEnd()
    {
        return transactionEnd;
    }

    public long getWalBaseOffset()
    {
        return walBaseOffset;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableId", tableId)
                .add("shardSize", shardUuids.size())
                .add("priority", priority)
                .add("bucketNumber", bucketNumber.isPresent() ? bucketNumber.getAsInt() : null)
                .omitNullValues()
                .toString();
    }
}
