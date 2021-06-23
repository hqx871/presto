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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.cstore.CStoreColumnHandle;
import com.facebook.presto.cstore.CStoreConnectorId;
import com.facebook.presto.cstore.backup.BackupManager;
import com.facebook.presto.cstore.backup.BackupStore;
import com.facebook.presto.cstore.filesystem.LocalCStoreDataEnvironment;
import com.facebook.presto.cstore.metadata.ShardManager;
import com.facebook.presto.cstore.metadata.ShardMetadata;
import com.facebook.presto.cstore.metadata.ShardRecorder;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.annotations.VisibleForTesting;
import github.cstore.coder.CompressFactory;
import github.cstore.column.BitmapColumnReader;
import github.cstore.column.CStoreColumnReader;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.joda.time.DateTimeZone;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.cstore.CStoreErrorCode.CSTORE_ERROR;
import static com.facebook.presto.cstore.CStoreErrorCode.CSTORE_LOCAL_DISK_FULL;
import static com.facebook.presto.cstore.CStoreErrorCode.CSTORE_RECOVERY_ERROR;
import static com.facebook.presto.cstore.CStoreErrorCode.CSTORE_RECOVERY_TIMEOUT;
import static com.facebook.presto.cstore.filesystem.FileSystemUtil.DEFAULT_CSTORE_CONTEXT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.airlift.units.DataSize.Unit.PETABYTE;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.joda.time.DateTimeZone.UTC;

public class CStoreStorageManager
        implements StorageManager
{
    private static final Logger log = Logger.get(CStoreStorageManager.class);
    // Raptor does not store time-related data types as they are in ORC.
    // They will be casted to BIGINT or INTEGER to avoid timezone conversion.
    // This is due to the historical reason of using the legacy ORC read/writer that does not support timestamp types.
    // In order to be consistent, we still enforce the conversion.
    // The following DEFAULT_STORAGE_TIMEZONE is not used by the optimized ORC read/writer given we never read/write timestamp types.
    public static final DateTimeZone DEFAULT_STORAGE_TIMEZONE = UTC;
    // TODO: do not limit the max size of blocks to read for now; enable the limit when the Hive connector is ready
    public static final DataSize HUGE_MAX_READ_BLOCK_SIZE = new DataSize(1, PETABYTE);

    private static final long MAX_ROWS = 1_000_000_000;

    private final String nodeId;
    private final StorageService storageService;
    private final Optional<BackupStore> backupStore;
    private final ReaderAttributes defaultReaderAttributes;
    private final BackupManager backupManager;
    private final ShardRecoveryManager recoveryManager;
    private final ShardRecorder shardRecorder;
    private final Duration recoveryTimeout;
    private final long maxShardRows;
    private final DataSize maxShardSize;
    private final DataSize minAvailableSpace;
    private final TypeManager typeManager;
    private final ExecutorService deletionExecutor;
    private final ExecutorService commitExecutor;
    private final CStoreDataEnvironment cStoreDataEnvironment;
    private final File dataDirectory;
    private final File stagingDirectory;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;
    private final ShardManager shardManager;
    private final CompressFactory compressorFactory;
    private final Map<UUID, ShardMetadata> shardMetadataMap;
    private final Map<UUID, CStoreShardLoader> shardLoaderMap;
    private final RawLocalFileSystem fileSystem;
    private final PageSorter pageSorter;

    @Inject
    public CStoreStorageManager(
            ShardManager shardManager,
            NodeManager nodeManager,
            StorageService storageService,
            Optional<BackupStore> backupStore,
            ReaderAttributes readerAttributes,
            StorageManagerConfig config,
            CStoreConnectorId connectorId,
            BackupManager backgroundBackupManager,
            ShardRecoveryManager recoveryManager,
            ShardRecorder shardRecorder,
            TypeManager typeManager,
            CStoreDataEnvironment cStoreDataEnvironment,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution,
            CompressFactory compressorFactory,
            PageSorter pageSorter)
    {
        this.shardManager = shardManager;
        this.nodeId = requireNonNull(nodeManager.getCurrentNode().getNodeIdentifier(), "nodeId is null");
        this.storageService = requireNonNull(storageService, "storageService is null");
        this.backupStore = requireNonNull(backupStore, "backupStore is null");
        this.defaultReaderAttributes = requireNonNull(readerAttributes, "readerAttributes is null");

        backupManager = requireNonNull(backgroundBackupManager, "backgroundBackupManager is null");
        this.recoveryManager = requireNonNull(recoveryManager, "recoveryManager is null");
        this.recoveryTimeout = requireNonNull(config.getShardRecoveryTimeout(), "shardRecoveryTimeout is null");
        this.functionMetadataManager = functionMetadataManager;
        this.standardFunctionResolution = standardFunctionResolution;
        this.compressorFactory = compressorFactory;
        this.pageSorter = pageSorter;

        checkArgument(config.getMaxShardRows() > 0, "maxShardRows must be > 0");
        this.maxShardRows = min(config.getMaxShardRows(), MAX_ROWS);
        this.maxShardSize = requireNonNull(config.getMaxShardSize(), "maxShardSize is null");
        this.minAvailableSpace = requireNonNull(config.getMinAvailableSpace(), "minAvailableSpace is null");
        this.shardRecorder = requireNonNull(shardRecorder, "shardRecorder is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.deletionExecutor = newFixedThreadPool(config.getDeletionThreads(), daemonThreadsNamed("raptor-delete-" + connectorId + "-%s"));
        this.commitExecutor = newCachedThreadPool(daemonThreadsNamed("raptor-commit-" + connectorId + "-%s"));
        this.cStoreDataEnvironment = requireNonNull(cStoreDataEnvironment, "orcDataEnvironment is null");
        this.stagingDirectory = new File(config.getStagingDirectory());
        assert this.stagingDirectory.exists() && this.stagingDirectory.isDirectory() : "staging work directory not exist";
        this.dataDirectory = new File(config.getDataDirectory());
        assert this.dataDirectory.exists() && this.dataDirectory.isDirectory();
        this.shardLoaderMap = new HashMap<>();
        this.shardMetadataMap = new HashMap<>();
        this.fileSystem = new LocalCStoreDataEnvironment().getFileSystem(DEFAULT_CSTORE_CONTEXT);
    }

    @PreDestroy
    public void shutdown()
    {
        deletionExecutor.shutdownNow();
        commitExecutor.shutdown();
        shardLoaderMap.values().forEach(CStoreShardLoader::close);
    }

    @Override
    public ConnectorPageSource getPageSource(
            UUID shardUuid,
            OptionalInt bucketNumber,
            List<CStoreColumnHandle> columnHandles,
            TupleDomain<CStoreColumnHandle> predicate,
            RowExpression filter,
            OptionalLong transactionId)
    {
        return CStorePageSource.create(this, typeManager, functionMetadataManager, standardFunctionResolution,
                columnHandles, shardUuid, filter, (int) getShardMeta(shardUuid).getRowCount());
    }

    @Override
    public UpdatablePageSource getUpdatablePageSource(UUID shardUuid, OptionalInt bucketNumber, List<CStoreColumnHandle> columnHandles,
            TupleDomain<CStoreColumnHandle> predicate, RowExpression filter, long transactionId, ConnectorPageSource source)
    {
        return new CStoreUpdatablePageSource(this, typeManager, functionMetadataManager, standardFunctionResolution,
                columnHandles, shardUuid, bucketNumber, transactionId, source, (int) getShardMeta(shardUuid).getRowCount());
    }

    private ShardMetadata getShardMeta(UUID shardUuid)
    {
        return shardMetadataMap.computeIfAbsent(shardUuid, k -> {
            ShardMetadata shardMetadata = shardManager.getShard(shardUuid);
            try {
                return loadShard(shardMetadata);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public StoragePageSink createStoragePageSink(
            long transactionId,
            OptionalInt bucketNumber,
            List<CStoreColumnHandle> columnHandles,
            boolean checkSpace)
    {
        if (checkSpace && storageService.getAvailableBytes() < minAvailableSpace.toBytes()) {
            throw new PrestoException(CSTORE_LOCAL_DISK_FULL, "Local disk is full on node " + nodeId);
        }
        return new CStoreStoragePageSink(fileSystem, transactionId, columnHandles, bucketNumber,
                maxShardRows, maxShardSize, shardRecorder, storageService, backupManager, nodeId,
                commitExecutor, cStoreDataEnvironment, stagingDirectory, backupStore, this, compressorFactory, typeManager);
    }

    @Override
    public StoragePageSink createStoragePageSink(long tableId, OptionalInt day, long transactionId, OptionalInt bucketNumber, List<CStoreColumnHandle> columnHandles, boolean checkSpace)
    {
        return createStoragePageSink(transactionId, bucketNumber, columnHandles, checkSpace);
    }

    @Override
    public StoragePageSink createStoragePageSink(long tableId, OptionalInt day, long transactionId, OptionalInt bucketNumber, List<CStoreColumnHandle> columnHandles, List<Long> sortFields, List<SortOrder> sortOrders, boolean checkSpace)
    {
        if (sortFields.isEmpty()) {
            return createStoragePageSink(tableId, day, transactionId, bucketNumber, columnHandles, checkSpace);
        }
        else {
            return new CStoreSortPageSink(pageSorter, columnHandles, sortFields, sortOrders, maxShardSize.toBytes(),
                    createStoragePageSink(transactionId, bucketNumber, columnHandles, checkSpace));
        }
    }

    @Override
    public void deleteShard(UUID shardUuid)
    {
    }

    private void writeShard(UUID shardUuid)
    {
        if (backupStore.isPresent() && !backupStore.get().shardExists(shardUuid)) {
            throw new PrestoException(CSTORE_ERROR, "Backup does not exist after write");
        }

        storageService.promoteFromStagingToStorage(shardUuid);
    }

    @Override
    //@PostConstruct
    public void setup()
            throws IOException
    {
        log.info("setup database...");
        Set<ShardMetadata> shardMetadataSet = shardManager.getNodeShards(nodeId);
        for (ShardMetadata shardMetadata : shardMetadataSet) {
            loadShard(shardMetadata);
        }
        log.info("database setup success");
    }

    private ShardMetadata loadShard(ShardMetadata shardMetadata)
            throws IOException
    {
        File file = openShard(shardMetadata.getShardUuid(), defaultReaderAttributes);
        CStoreShardLoader shardLoader = new CStoreShardLoader(file, compressorFactory, typeManager);
        shardLoader.setup();
        shardLoaderMap.put(shardMetadata.getShardUuid(), shardLoader);
        shardMetadataMap.put(shardMetadata.getShardUuid(), shardMetadata);
        return shardMetadata;
    }

    private CStoreShardLoader getShardLoader(UUID shardUuid)
    {
        ShardMetadata shardMetadata = getShardMeta(shardUuid);
        return shardLoaderMap.get(shardMetadata.getShardUuid());
    }

    public CStoreColumnReader getColumnReader(UUID shardUuid, long columnId)
    {
        return getShardLoader(shardUuid).getColumnReaderMap().get(columnId).build();
    }

    public BitmapColumnReader getBitmapReader(UUID shardUuid, long columnId)
    {
        return getShardLoader(shardUuid).getBitmapReaderMap().get(columnId).build();
    }

    @VisibleForTesting
    private File openShard(UUID shardUuid, ReaderAttributes readerAttributes)
    {
        Path file = storageService.getStorageFile(shardUuid);

        boolean exists;
        try {
            exists = fileSystem.exists(file);
        }
        catch (IOException e) {
            throw new PrestoException(CSTORE_ERROR, "Error locating file " + file, e.getCause());
        }

        if (!exists && backupStore.isPresent()) {
            try {
                Future<?> future = recoveryManager.recoverShard(shardUuid);
                future.get(recoveryTimeout.toMillis(), TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            catch (ExecutionException e) {
                if (e.getCause() != null) {
                    throwIfInstanceOf(e.getCause(), PrestoException.class);
                }
                throw new PrestoException(CSTORE_RECOVERY_ERROR, "Error recovering shard " + shardUuid, e.getCause());
            }
            catch (TimeoutException e) {
                throw new PrestoException(CSTORE_RECOVERY_TIMEOUT, "Shard is being recovered from backup. Please retry in a few minutes: " + shardUuid);
            }
        }
        return fileSystem.pathToFile(file);
    }
}
