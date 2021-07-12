package com.facebook.presto.cstore.storage;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.cstore.CStoreColumnHandle;
import com.facebook.presto.cstore.CStoreConnectorId;
import com.facebook.presto.cstore.backup.BackupManager;
import com.facebook.presto.cstore.backup.BackupStore;
import com.facebook.presto.cstore.filesystem.LocalCStoreDataEnvironment;
import com.facebook.presto.cstore.metadata.MetadataDao;
import com.facebook.presto.cstore.metadata.ShardManager;
import com.facebook.presto.cstore.metadata.ShardMetadata;
import com.facebook.presto.cstore.metadata.ShardRecorder;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;
import github.cstore.coder.CompressFactory;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.hadoop.fs.RawLocalFileSystem;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.cstore.filesystem.FileSystemUtil.DEFAULT_CSTORE_CONTEXT;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class MemoryStorageManager
        implements StorageManager
{
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
    private final CStoreStorageManager delegate;
    private final BlockEncodingSerde blockEncodingSerde;
    private final PagesSerdeFactory pagesSerdeFactory;
    private final ConcurrentMap<Object, MemoryShardStore> bucketPageBuffers;
    private final ConcurrentMap<UUID, MemoryShardStore> pageBuffers;
    private final MetadataDao metadataDao;

    @Inject
    public MemoryStorageManager(
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
            BlockEncodingSerde blockEncodingSerde,
            CStoreStorageManager delegate,
            MetadataDao metadataDao)
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
        this.delegate = delegate;
        this.metadataDao = metadataDao;

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
        this.blockEncodingSerde = blockEncodingSerde;
        this.pagesSerdeFactory = new PagesSerdeFactory(blockEncodingSerde, true, true);
        this.fileSystem = new LocalCStoreDataEnvironment().getFileSystem(DEFAULT_CSTORE_CONTEXT);
        this.bucketPageBuffers = new ConcurrentHashMap<>();
        this.pageBuffers = new ConcurrentHashMap<>();
    }

    @Override
    public ConnectorPageSource getPageSource(UUID shardUuid, OptionalInt bucketNumber, List<CStoreColumnHandle> columnHandles,
            TupleDomain<CStoreColumnHandle> predicate, RowExpression filter, OptionalLong transactionId)
    {
        MemoryShardStore pageBuffer = pageBuffers.get(shardUuid);
        if (pageBuffer == null) {
            return delegate.getPageSource(shardUuid, bucketNumber, columnHandles, predicate, filter, transactionId);
        }
        Iterator<Page> pageIterator = ImmutableList.copyOf(pageBuffer.getPages()).iterator();
        return new MemoryPageSource(pageIterator, pageBuffer, columnHandles);
    }

    @Override
    public UpdatablePageSource getUpdatablePageSource(UUID shardUuid, OptionalInt bucketNumber, List<CStoreColumnHandle> columnHandles, TupleDomain<CStoreColumnHandle> predicate, RowExpression filter, long transactionId, ConnectorPageSource source)
    {
        return delegate.getUpdatablePageSource(shardUuid, bucketNumber, columnHandles, predicate, filter, transactionId, source);
    }

    @Override
    public CStoreStoragePageFileSink createStoragePageFileSink(long transactionId, OptionalInt bucketNumber, List<CStoreColumnHandle> columnHandles, boolean checkSpace)
    {
        return delegate.createStoragePageFileSink(transactionId, bucketNumber, columnHandles, checkSpace);
    }

    @Override
    public StoragePageBufferSink createStoragePageSortSink(long tableId, OptionalInt day, long transactionId, OptionalInt bucketNumber, List<CStoreColumnHandle> columnHandles, List<Long> sortFields, List<SortOrder> sortOrders, boolean checkSpace)
    {
        List<Object> key = ImmutableList.of(OptionalLong.of(tableId), day, bucketNumber);
        MemoryShardStore CStoreShardSink = bucketPageBuffers.get(key);
        boolean newShard = CStoreShardSink == null;
        if (newShard) {
            UUID shardUuid = UUID.randomUUID();
            shardManager.recordCreatedShard(transactionId, shardUuid);
            if (sortFields.isEmpty()) {
                CStoreShardSink = new MemoryShardSimpleStore(shardUuid, maxShardSize.toBytes(), columnHandles, OptionalLong.of(tableId), day, bucketNumber);
            }
            else {
                CStoreShardSink = new MemoryShardSortStore(shardUuid, maxShardSize.toBytes(), columnHandles, sortFields, OptionalLong.of(tableId), day, bucketNumber);
            }
            CStoreShardSink = new MemoryShardWalStore(Paths.get(stagingDirectory.getAbsolutePath(), shardUuid.toString()).toUri(), pagesSerdeFactory, CStoreShardSink, true);
            pageBuffers.put(shardUuid, CStoreShardSink);
            bucketPageBuffers.put(key, CStoreShardSink);
        }

        return new StoragePageBufferSink(transactionId, bucketNumber, maxShardRows, maxShardSize,
                shardRecorder, nodeId, delegate.createStoragePageFileSink(transactionId, bucketNumber, columnHandles, checkSpace),
                maxShardSize.toBytes(), CStoreShardSink, newShard);
    }

    @Override
    public void deleteShard(UUID shardUuid)
    {
        MemoryShardStore pageBuffer = pageBuffers.remove(shardUuid);
        if (pageBuffer != null) {
            Object key = ImmutableList.of(pageBuffer.getTableId(), pageBuffer.getPartitionDay(),
                    pageBuffer.getBucketNumber());
            bucketPageBuffers.remove(key);
            pageBuffer.reset();
        }
        else {
            delegate.deleteShard(shardUuid);
        }
    }

    @Override
    @PostConstruct
    public void setup()
            throws IOException
    {
        delegate.setup();
        Set<ShardMetadata> shardMetadataSet = shardManager.getNodeShards(nodeId);
        shardMetadataSet = shardMetadataSet.stream()
                .filter(ShardMetadata::isMutable)
                .collect(Collectors.toSet());
        for (ShardMetadata shardMetadata : shardMetadataSet) {
            MemoryShardStore CStoreShardSink = recoverMemoryPageBuffer(shardMetadata);
            pageBuffers.put(CStoreShardSink.getUuid(), CStoreShardSink);
            Object bucketKey = ImmutableList.of(CStoreShardSink.getTableId(), CStoreShardSink.getPartitionDay(), CStoreShardSink.getBucketNumber());
            bucketPageBuffers.put(bucketKey, CStoreShardSink);
        }
    }

    private MemoryShardStore recoverMemoryPageBuffer(ShardMetadata shardMetadata)
    {
        URI uri = Paths.get(stagingDirectory.getAbsolutePath(), shardMetadata.getShardUuid().toString()).toUri();
        return MemoryShardWalStore.recoverFromUri(uri, maxShardSize.toBytes(), pagesSerdeFactory);
    }

    @Override
    @PreDestroy
    public void shutdown()
    {
        delegate.shutdown();
    }
}
