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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cstore.CStoreColumnHandle;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.relation.RowExpression;
import github.cstore.column.BitmapColumnReader;
import github.cstore.column.CStoreColumnReader;

import java.io.IOException;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.UUID;

public interface StorageManager
{
    ConnectorPageSource getPageSource(
            UUID shardUuid,
            OptionalInt bucketNumber,
            List<CStoreColumnHandle> columnHandles,
            TupleDomain<CStoreColumnHandle> predicate,
            RowExpression filter,
            OptionalLong transactionId);

    StoragePageSink createStoragePageSink(
            HdfsContext hdfsContext,
            long transactionId,
            OptionalInt bucketNumber,
            List<CStoreColumnHandle> columnHandles,
            boolean checkSpace);

    void setup()
            throws IOException;

    void shutdown();

    CStoreColumnReader getColumnReader(UUID shard, long columnId);

    BitmapColumnReader getBitmapReader(UUID shard, long columnId);
}
