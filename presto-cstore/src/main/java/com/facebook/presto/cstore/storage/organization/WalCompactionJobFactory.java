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

import com.facebook.presto.cstore.metadata.ForMetadata;
import com.facebook.presto.cstore.metadata.MetadataDao;
import com.facebook.presto.cstore.metadata.ShardManager;
import com.facebook.presto.cstore.storage.StorageManager;
import com.facebook.presto.cstore.storage.WriteAheadLogManager;
import org.skife.jdbi.v2.IDBI;

import javax.inject.Inject;

import static com.facebook.presto.cstore.util.DatabaseUtil.onDemandDao;
import static java.util.Objects.requireNonNull;

public class WalCompactionJobFactory
        implements JobFactory<WalCompactionSet>
{
    private final MetadataDao metadataDao;
    private final ShardManager shardManager;
    private final ShardCompactor compactor;
    private final StorageManager storageManager;
    private final WriteAheadLogManager walManager;

    @Inject
    public WalCompactionJobFactory(@ForMetadata IDBI dbi, ShardManager shardManager, ShardCompactor compactor,
            StorageManager storageManager, WriteAheadLogManager walManager)
    {
        this.storageManager = storageManager;
        this.walManager = walManager;
        requireNonNull(dbi, "dbi is null");
        this.metadataDao = onDemandDao(dbi, MetadataDao.class);
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.compactor = requireNonNull(compactor, "compactor is null");
    }

    @Override
    public Runnable create(WalCompactionSet organizationSet)
    {
        return new WalCompactionJob(organizationSet, metadataDao, shardManager, compactor, storageManager,
                walManager);
    }
}
