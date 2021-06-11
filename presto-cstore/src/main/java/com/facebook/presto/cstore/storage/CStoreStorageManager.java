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
import com.facebook.presto.common.Page;
import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.cstore.CStoreColumnHandle;
import com.facebook.presto.cstore.CStoreConnectorId;
import com.facebook.presto.cstore.backup.BackupManager;
import com.facebook.presto.cstore.backup.BackupStore;
import com.facebook.presto.cstore.filesystem.LocalCStoreDataEnvironment;
import com.facebook.presto.cstore.metadata.ColumnStats;
import com.facebook.presto.cstore.metadata.ShardInfo;
import com.facebook.presto.cstore.metadata.ShardManager;
import com.facebook.presto.cstore.metadata.ShardMetadata;
import com.facebook.presto.cstore.metadata.ShardRecorder;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import github.cstore.coder.CompressFactory;
import github.cstore.column.BitmapColumnReader;
import github.cstore.column.CStoreColumnReader;
import github.cstore.meta.ShardColumn;
import github.cstore.meta.ShardMeta;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.joda.time.DateTimeZone;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.facebook.airlift.concurrent.MoreFutures.allAsList;
import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.cstore.CStoreErrorCode.CSTORE_ERROR;
import static com.facebook.presto.cstore.CStoreErrorCode.CSTORE_LOCAL_DISK_FULL;
import static com.facebook.presto.cstore.CStoreErrorCode.CSTORE_RECOVERY_ERROR;
import static com.facebook.presto.cstore.CStoreErrorCode.CSTORE_RECOVERY_TIMEOUT;
import static com.facebook.presto.cstore.filesystem.FileSystemUtil.DEFAULT_CSTORE_CONTEXT;
import static com.facebook.presto.cstore.filesystem.FileSystemUtil.xxhash64;
import static com.facebook.presto.cstore.storage.ShardStats.computeColumnStats;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.airlift.units.DataSize.Unit.PETABYTE;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.toList;
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
    private final Map<UUID, ShardMeta> shardMetaMap;
    private final Map<Long, CStoreColumnReader.Builder> columnReaderMap;
    private final Map<Long, BitmapColumnReader.Builder> bitmapReaderMap;
    private final RawLocalFileSystem fileSystem;

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
            CompressFactory compressorFactory)
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
        this.columnReaderMap = new HashMap<>();
        this.bitmapReaderMap = new HashMap<>();
        this.shardMetaMap = new HashMap<>();
        this.fileSystem = new LocalCStoreDataEnvironment().getFileSystem(DEFAULT_CSTORE_CONTEXT);

        try {
            setup();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        deletionExecutor.shutdownNow();
        commitExecutor.shutdown();
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
        return new CStorePageSource(this, typeManager, functionMetadataManager, standardFunctionResolution,
                columnHandles, shardUuid, filter, shardMetaMap.get(shardUuid).getRowCnt(), predicate);
    }

    @Override
    public StoragePageSink createStoragePageSink(
            HdfsContext hdfsContext,
            long transactionId,
            OptionalInt bucketNumber,
            List<CStoreColumnHandle> columnHandles,
            boolean checkSpace)
    {
        if (checkSpace && storageService.getAvailableBytes() < minAvailableSpace.toBytes()) {
            throw new PrestoException(CSTORE_LOCAL_DISK_FULL, "Local disk is full on node " + nodeId);
        }
        return new CStoreStoragePageSink(cStoreDataEnvironment.getFileSystem(hdfsContext), transactionId, columnHandles, bucketNumber);
    }

    private void writeShard(UUID shardUuid)
    {
        if (backupStore.isPresent() && !backupStore.get().shardExists(shardUuid)) {
            throw new PrestoException(CSTORE_ERROR, "Backup does not exist after write");
        }

        storageService.promoteFromStagingToStorage(shardUuid);
    }

    @Override
    public void setup()
            throws IOException
    {
        log.info("setup database...");
        Set<ShardMetadata> shardMetadataSet = shardManager.getNodeShards(nodeId);
        for (ShardMetadata shardMetadata : shardMetadataSet) {
            File file = openShard(shardMetadata.getShardUuid(), defaultReaderAttributes);
            CStoreTableLoader tableLoader = new CStoreTableLoader(file, compressorFactory);
            tableLoader.setup();
            ShardMeta shardMeta = tableLoader.getShardMeta();
            columnReaderMap.putAll(tableLoader.getColumnReaderMap());
            bitmapReaderMap.putAll(tableLoader.getBitmapReaderMap());
            shardMetaMap.put(shardMeta.getUuid(), shardMeta);
        }
        log.info("database setup success");
    }

    @Override
    public CStoreColumnReader getColumnReader(UUID shardUuid, long columnId)
    {
        return columnReaderMap.get(columnId).build();
    }

    @Override
    public BitmapColumnReader getBitmapReader(UUID shardUuid, long columnId)
    {
        return bitmapReaderMap.get(columnId).build();
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

    private ShardInfo createShardInfo(UUID shardUuid, OptionalInt bucketNumber, Path file, Set<String> nodes, long rowCount, long uncompressedSize)
    {
        try {
            return new ShardInfo(shardUuid, bucketNumber, nodes, computeShardStats(file), rowCount, fileSystem.getFileStatus(file).getLen(), uncompressedSize, xxhash64(fileSystem, file));
        }
        catch (IOException e) {
            throw new PrestoException(CSTORE_ERROR, "Failed to get file status: " + file, e);
        }
    }

    private List<ColumnStats> computeShardStats(Path file)
    {
        try {
            ImmutableList.Builder<ColumnStats> list = ImmutableList.builder();
            CStoreTableLoader tableLoader = new CStoreTableLoader(fileSystem.pathToFile(file), compressorFactory);
            tableLoader.setup();
            for (ShardColumn info : tableLoader.getShardMeta().getColumns()) {
                CStoreColumnReader cStoreColumnReader = tableLoader.getColumnReaderMap().get(info.getColumnId()).build();
                Type type = CStoreTableLoader.getType(info.getTypeName());
                computeColumnStats(cStoreColumnReader, info.getColumnId(), type).ifPresent(list::add);
            }
            return list.build();
        }
        catch (IOException e) {
            throw new PrestoException(CSTORE_ERROR, "Failed to read file: " + file, e);
        }
    }

    private class CStoreStoragePageSink
            implements StoragePageSink
    {
        private final long transactionId;
        private final List<Long> columnIds;
        private final List<Type> columnTypes;
        private final OptionalInt bucketNumber;

        private final List<Path> stagingFiles = new ArrayList<>();
        private final List<ShardInfo> shards = new ArrayList<>();
        private final List<CompletableFuture<?>> futures = new ArrayList<>();
        private final FileSystem fileSystem;

        private boolean committed;
        private FileWriter writer;
        private UUID shardUuid;

        public CStoreStoragePageSink(
                FileSystem fileSystem,
                long transactionId,
                List<CStoreColumnHandle> columnHandles,
                OptionalInt bucketNumber)
        {
            this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
            this.transactionId = transactionId;
            this.columnIds = columnHandles.stream().map(CStoreColumnHandle::getColumnId).collect(Collectors.toList());
            this.columnTypes = columnHandles.stream().map(CStoreColumnHandle::getColumnType).collect(toList());
            this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        }

        @Override
        public void appendPages(List<Page> pages)
        {
            createWriterIfNecessary();
            writer.appendPages(pages);
        }

        @Override
        public void appendPages(List<Page> inputPages, int[] pageIndexes, int[] positionIndexes)
        {
            createWriterIfNecessary();
            writer.appendPages(inputPages, pageIndexes, positionIndexes);
        }

        @Override
        public boolean isFull()
        {
            if (writer == null) {
                return false;
            }
            return (writer.getRowCount() >= maxShardRows) || (writer.getUncompressedSize() >= maxShardSize.toBytes());
        }

        @Override
        public void flush()
        {
            if (writer != null) {
                try {
                    writer.close();
                }
                catch (IOException e) {
                    throw new PrestoException(CSTORE_ERROR, "Failed to close writer", e);
                }

                shardRecorder.recordCreatedShard(transactionId, shardUuid);

                Path stagingFile = storageService.getStagingFile(shardUuid);
                futures.add(backupManager.submit(shardUuid, stagingFile));

                Set<String> nodes = ImmutableSet.of(nodeId);
                long rowCount = writer.getRowCount();
                long uncompressedSize = writer.getUncompressedSize();

                shards.add(createShardInfo(shardUuid, bucketNumber, stagingFile, nodes, rowCount, uncompressedSize));

                writer = null;
                shardUuid = null;
            }
        }

        @Override
        public CompletableFuture<List<ShardInfo>> commit()
        {
            checkState(!committed, "already committed");
            committed = true;

            flush();

            return allAsList(futures).thenApplyAsync(ignored -> {
                for (ShardInfo shard : shards) {
                    writeShard(shard.getShardUuid());
                }
                return ImmutableList.copyOf(shards);
            }, commitExecutor);
        }

        @SuppressWarnings("ResultOfMethodCallIgnored")
        @Override
        public void rollback()
        {
            try {
                if (writer != null) {
                    try {
                        writer.close();
                    }
                    catch (IOException e) {
                        throw new PrestoException(CSTORE_ERROR, "Failed to close writer", e);
                    }
                    finally {
                        writer = null;
                    }
                }
            }
            finally {
                for (Path file : stagingFiles) {
                    try {
                        fileSystem.delete(file, false);
                    }
                    catch (IOException e) {
                        // ignore
                    }
                }

                // cancel incomplete backup jobs
                futures.forEach(future -> future.cancel(true));

                // delete completed backup shards
                backupStore.ifPresent(backupStore -> {
                    for (ShardInfo shard : shards) {
                        backupStore.deleteShard(shard.getShardUuid());
                    }
                });
            }
        }

        private void createWriterIfNecessary()
        {
            if (writer == null) {
                shardUuid = UUID.randomUUID();
                Path stagingFile = storageService.getStagingFile(shardUuid);
                storageService.createParents(stagingFile);
                stagingFiles.add(stagingFile);
                DataSink sink;
                try {
                    sink = cStoreDataEnvironment.createDataSink(fileSystem, stagingFile);
                }
                catch (IOException e) {
                    throw new PrestoException(CSTORE_ERROR, format("Failed to create staging file %s", stagingFile), e);
                }
                writer = new CStoreFileWriter(columnIds, columnTypes, stagingDirectory, sink);
            }
        }
    }
}
