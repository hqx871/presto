package com.facebook.presto.cstore.storage;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.cstore.CStoreColumnHandle;
import com.facebook.presto.cstore.backup.BackupManager;
import com.facebook.presto.cstore.backup.BackupStore;
import com.facebook.presto.cstore.metadata.ColumnStats;
import com.facebook.presto.cstore.metadata.ShardInfo;
import com.facebook.presto.cstore.metadata.ShardRecorder;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import github.cstore.coder.CompressFactory;
import github.cstore.column.CStoreColumnReader;
import github.cstore.meta.ShardColumn;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.facebook.airlift.concurrent.MoreFutures.allAsList;
import static com.facebook.presto.cstore.CStoreErrorCode.CSTORE_ERROR;
import static com.facebook.presto.cstore.CStoreErrorCode.CSTORE_WRITER_DATA_ERROR;
import static com.facebook.presto.cstore.filesystem.FileSystemUtil.xxhash64;
import static com.facebook.presto.cstore.storage.ShardStats.computeColumnStats;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class CStorePageFileSink
        implements ConnectorPageSink
{
    private final long transactionId;
    private final List<Long> columnIds;
    private final List<Type> columnTypes;
    private final OptionalInt bucketNumber;
    private final long maxShardRows;
    private final DataSize maxShardSize;
    private final ShardRecorder shardRecorder;
    private final StorageService storageService;
    private final BackupManager backupManager;
    private final String nodeId;
    private final ExecutorService commitExecutor;
    private final CStoreDataEnvironment cStoreDataEnvironment;
    private final File stagingDirectory;
    private final Optional<BackupStore> backupStore;
    private final CStoreStorageManager storeStorageManager;

    private final List<Path> stagingFiles = new ArrayList<>();
    private final List<ShardInfo> shards = new ArrayList<>();
    private final List<CompletableFuture<?>> futures = new ArrayList<>();
    private final RawLocalFileSystem fileSystem;
    private final CompressFactory compressorFactory;
    private final TypeManager typeManager;

    private boolean committed;
    private CStoreShardFileSink shardFileSink;
    //private ShardFileWriter writer;
    //private UUID shardUuid;

    public CStorePageFileSink(
            RawLocalFileSystem fileSystem,
            long transactionId,
            List<CStoreColumnHandle> columnHandles,
            OptionalInt bucketNumber,
            long maxShardRows, DataSize maxShardSize,
            ShardRecorder shardRecorder,
            StorageService storageService,
            BackupManager backupManager,
            String nodeId,
            ExecutorService commitExecutor,
            CStoreDataEnvironment cStoreDataEnvironment,
            File stagingDirectory,
            Optional<BackupStore> backupStore,
            CStoreStorageManager storeStorageManager,
            CompressFactory compressorFactory,
            TypeManager typeManager)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.transactionId = transactionId;
        this.columnIds = columnHandles.stream().map(CStoreColumnHandle::getColumnId).collect(Collectors.toList());
        this.columnTypes = columnHandles.stream().map(CStoreColumnHandle::getColumnType).collect(toList());
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.maxShardRows = maxShardRows;
        this.maxShardSize = maxShardSize;
        this.shardRecorder = shardRecorder;
        this.storageService = storageService;
        this.backupManager = backupManager;
        this.nodeId = nodeId;
        this.commitExecutor = commitExecutor;
        this.cStoreDataEnvironment = cStoreDataEnvironment;
        this.stagingDirectory = stagingDirectory;
        this.backupStore = backupStore;
        this.storeStorageManager = storeStorageManager;
        this.compressorFactory = compressorFactory;
        this.typeManager = typeManager;
    }

    //@Override
    public boolean isShardFileFull()
    {
        if (shardFileSink == null) {
            return false;
        }
        return (shardFileSink.getRowCount() >= maxShardRows) || (shardFileSink.getUncompressedSize() >= maxShardSize.toBytes());
    }

    //@Override
    public void flushFileSink()
    {
        if (shardFileSink != null) {
            try {
                shardFileSink.close();
            }
            catch (IOException e) {
                throw new PrestoException(CSTORE_ERROR, "Failed to close writer", e);
            }

            shardRecorder.recordCreatedShard(transactionId, shardFileSink.getUuid());

            Path stagingFile = storageService.getStagingFile(shardFileSink.getUuid());
            futures.add(backupManager.submit(shardFileSink.getUuid(), stagingFile));

            Set<String> nodes = ImmutableSet.of(nodeId);
            long rowCount = shardFileSink.getRowCount();
            long uncompressedSize = shardFileSink.getUncompressedSize();

            shards.add(createShardInfo(shardFileSink.getUuid(), bucketNumber, stagingFile, nodes, rowCount, uncompressedSize));

            shardFileSink = null;
        }
    }

    //@Override
    public CompletableFuture<List<ShardInfo>> commit()
    {
        checkState(!committed, "already committed");
        committed = true;

        flushFileSink();

        return allAsList(futures).thenApplyAsync(ignored -> {
            for (ShardInfo shard : shards) {
                writeShard(shard.getShardUuid());
            }
            return ImmutableList.copyOf(shards);
        }, commitExecutor);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void abort()
    {
        try {
            if (shardFileSink != null) {
                try {
                    shardFileSink.close();
                }
                catch (IOException e) {
                    throw new PrestoException(CSTORE_ERROR, "Failed to close writer", e);
                }
                finally {
                    shardFileSink = null;
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

    private void createNewShardFileIfNecessary()
    {
        if (shardFileSink == null) {
            UUID shardUuid = UUID.randomUUID();
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
            List<String> columnNames = columnIds.stream().map(Object::toString).collect(Collectors.toList());
            shardFileSink = new CStoreShardFileSink(columnIds, stagingDirectory, sink, columnNames, columnTypes, shardUuid);
            shardFileSink.setup();
        }
    }

    private void writeShard(UUID shardUuid)
    {
        if (backupStore.isPresent() && !backupStore.get().shardExists(shardUuid)) {
            throw new PrestoException(CSTORE_ERROR, "Backup does not exist after write");
        }

        storageService.promoteFromStagingToStorage(shardUuid);
    }

    private ShardInfo createShardInfo(UUID shardUuid, OptionalInt bucketNumber, Path file, Set<String> nodes, long rowCount, long uncompressedSize)
    {
        try {
            return new ShardInfo(shardUuid, bucketNumber, nodes, computeShardStats(file), rowCount, fileSystem.getFileStatus(file).getLen(), uncompressedSize, xxhash64(fileSystem, file), false);
        }
        catch (IOException e) {
            throw new PrestoException(CSTORE_ERROR, "Failed to get file status: " + file, e);
        }
    }

    private List<ColumnStats> computeShardStats(Path file)
    {
        try {
            ImmutableList.Builder<ColumnStats> list = ImmutableList.builder();
            CStoreShardLoader tableLoader = new CStoreShardLoader(fileSystem.pathToFile(file), compressorFactory, typeManager);
            tableLoader.setup();
            for (ShardColumn info : tableLoader.getShardSchema().getColumns()) {
                CStoreColumnReader cStoreColumnReader = tableLoader.getColumnReaderSuppliers().get(info.getColumnId()).get();
                Type type = getType(info.getTypeName());
                computeColumnStats(cStoreColumnReader, info.getColumnId(), type).ifPresent(list::add);
            }
            return list.build();
        }
        catch (IOException e) {
            throw new PrestoException(CSTORE_ERROR, "Failed to read file: " + file, e);
        }
    }

    private Type getType(String sign)
    {
        return typeManager.getType(TypeSignature.parseTypeSignature(sign));
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        try {
            if (isShardFileFull()) {
                flushFileSink();
            }
            createNewShardFileIfNecessary();
            shardFileSink.appendPage(page);
        }
        catch (UncheckedIOException e) {
            throw new PrestoException(CSTORE_WRITER_DATA_ERROR, e);
        }
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        CompletableFuture<List<ShardInfo>> futureShards = commit();
        return futureShards.thenApply(shards -> shards.stream()
                .map(shard -> Slices.wrappedBuffer(ShardInfo.JSON_CODEC.toJsonBytes(shard)))
                .collect(toList()));
    }
}
