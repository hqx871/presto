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
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import github.cstore.coder.CompressFactory;
import github.cstore.column.CStoreColumnReader;
import github.cstore.meta.ShardColumn;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
import static com.facebook.presto.cstore.filesystem.FileSystemUtil.xxhash64;
import static com.facebook.presto.cstore.storage.ShardStats.computeColumnStats;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

class CStoreStoragePageSink
        implements StoragePageSink
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
    private FileWriter writer;
    private UUID shardUuid;

    public CStoreStoragePageSink(
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
            CStoreStorageManager storeStorageManager, CompressFactory compressorFactory, TypeManager typeManager)
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
            writer = new CStoreFileWriter(columnIds, columnTypes, stagingDirectory, shardUuid, sink);
            writer.setup();
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
                CStoreColumnReader cStoreColumnReader = tableLoader.getColumnReaderMap().get(info.getColumnId()).build();
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
}
