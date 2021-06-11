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
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.RowFieldName;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.cstore.CStoreColumnHandle;
import com.facebook.presto.cstore.CStoreConnectorId;
import com.facebook.presto.cstore.backup.BackupManager;
import com.facebook.presto.cstore.backup.BackupStore;
import com.facebook.presto.cstore.metadata.ColumnInfo;
import com.facebook.presto.cstore.metadata.ColumnStats;
import com.facebook.presto.cstore.metadata.MetadataDao;
import com.facebook.presto.cstore.metadata.ShardInfo;
import com.facebook.presto.cstore.metadata.ShardManager;
import com.facebook.presto.cstore.metadata.ShardMetadata;
import com.facebook.presto.cstore.metadata.ShardRecorder;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.orc.StripeMetadataSource;
import com.facebook.presto.orc.cache.OrcFileTailSource;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.OrcType;
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
import org.joda.time.DateTimeZone;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.Closeable;
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
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.cstore.CStoreErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.cstore.CStoreErrorCode.RAPTOR_LOCAL_DISK_FULL;
import static com.facebook.presto.cstore.CStoreErrorCode.RAPTOR_RECOVERY_ERROR;
import static com.facebook.presto.cstore.CStoreErrorCode.RAPTOR_RECOVERY_TIMEOUT;
import static com.facebook.presto.cstore.filesystem.FileSystemUtil.xxhash64;
import static com.facebook.presto.cstore.storage.ShardStats.computeColumnStats;
import static com.facebook.presto.cstore.storage.StorageManagerConfig.OrcOptimizedWriterStage.ENABLED_AND_VALIDATED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
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
    private final CompressionKind compression;
    private final StorageManagerConfig.OrcOptimizedWriterStage orcOptimizedWriterStage;
    private final TypeManager typeManager;
    private final ExecutorService deletionExecutor;
    private final ExecutorService commitExecutor;
    private final OrcDataEnvironment orcDataEnvironment;
    private final OrcWriterStats stats = new OrcWriterStats();
    private final OrcFileTailSource orcFileTailSource;
    private final StripeMetadataSource stripeMetadataSource;
    private final File dataDirectory;
    private final File stagingDirectory;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;
    private final MetadataDao metadataDao;
    private final ShardManager shardManager;
    private final CompressFactory compressorFactory;
    private final CStoreColumnLoader columnLoader;
    private final Map<UUID, ShardMeta> shardMetaMap;
    private final Map<Long, CStoreColumnReader.Builder> columnReaderMap;
    private final Map<Long, BitmapColumnReader.Builder> bitmapReaderMap;

    @Inject
    public CStoreStorageManager(
            ShardManager shardManager,
            MetadataDao metadataDao,
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
            OrcDataEnvironment orcDataEnvironment,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSource stripeMetadataSource,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution,
            CompressFactory compressorFactory, CStoreColumnLoader columnLoader)
    {
        this.shardManager = shardManager;
        this.metadataDao = metadataDao;
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
        this.columnLoader = columnLoader;

        checkArgument(config.getMaxShardRows() > 0, "maxShardRows must be > 0");
        this.maxShardRows = min(config.getMaxShardRows(), MAX_ROWS);
        this.maxShardSize = requireNonNull(config.getMaxShardSize(), "maxShardSize is null");
        this.minAvailableSpace = requireNonNull(config.getMinAvailableSpace(), "minAvailableSpace is null");
        this.shardRecorder = requireNonNull(shardRecorder, "shardRecorder is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.deletionExecutor = newFixedThreadPool(config.getDeletionThreads(), daemonThreadsNamed("raptor-delete-" + connectorId + "-%s"));
        this.commitExecutor = newCachedThreadPool(daemonThreadsNamed("raptor-commit-" + connectorId + "-%s"));
        this.compression = requireNonNull(config.getOrcCompressionKind(), "compression is null");
        this.orcOptimizedWriterStage = requireNonNull(config.getOrcOptimizedWriterStage(), "orcOptimizedWriterStage is null");
        this.orcDataEnvironment = requireNonNull(orcDataEnvironment, "orcDataEnvironment is null");
        this.stagingDirectory = new File(config.getStagingDirectory());
        assert this.stagingDirectory.exists();
        this.dataDirectory = new File(config.getDataDirectory());
        assert this.dataDirectory.exists();
        this.orcFileTailSource = requireNonNull(orcFileTailSource, "orcFileTailSource is null");
        this.stripeMetadataSource = requireNonNull(stripeMetadataSource, "stripeMetadataSource is null");
        this.columnReaderMap = new HashMap<>();
        this.bitmapReaderMap = new HashMap<>();
        this.shardMetaMap = new HashMap<>();

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
            throw new PrestoException(RAPTOR_LOCAL_DISK_FULL, "Local disk is full on node " + nodeId);
        }
        return new CStoreStoragePageSink(orcDataEnvironment.getFileSystem(hdfsContext), transactionId, columnHandles, bucketNumber);
    }

    private void writeShard(UUID shardUuid)
    {
        if (backupStore.isPresent() && !backupStore.get().shardExists(shardUuid)) {
            throw new PrestoException(RAPTOR_ERROR, "Backup does not exist after write");
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
            File file = openShard(null, shardMetadata.getShardUuid(), null);
            TableLoader tableLoader = new TableLoader(file, compressorFactory);
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
    private File openShard(FileSystem fileSystem, UUID shardUuid, ReaderAttributes readerAttributes)
    {
        Path file = storageService.getStorageFile(shardUuid);

        boolean exists;
        try {
            exists = fileSystem.exists(file);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Error locating file " + file, e.getCause());
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
                throw new PrestoException(RAPTOR_RECOVERY_ERROR, "Error recovering shard " + shardUuid, e.getCause());
            }
            catch (TimeoutException e) {
                throw new PrestoException(RAPTOR_RECOVERY_TIMEOUT, "Shard is being recovered from backup. Please retry in a few minutes: " + shardUuid);
            }
        }

        return new File(file.toUri());
    }

    private ShardInfo createShardInfo(FileSystem fileSystem, UUID shardUuid, OptionalInt bucketNumber, Path file, Set<String> nodes, long rowCount, long uncompressedSize)
    {
        try {
            return new ShardInfo(shardUuid, bucketNumber, nodes, computeShardStats(fileSystem, file), rowCount, fileSystem.getFileStatus(file).getLen(), uncompressedSize, xxhash64(fileSystem, file));
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to get file status: " + file, e);
        }
    }

    private List<ColumnStats> computeShardStats(FileSystem fileSystem, Path file)
    {
        try {
            ImmutableList.Builder<ColumnStats> list = ImmutableList.builder();
            TableLoader tableLoader = new TableLoader(new File(file.toUri()), compressorFactory);
            for (ShardColumn info : tableLoader.getShardMeta().getColumns()) {
                CStoreColumnReader cStoreColumnReader = tableLoader.getColumnReaderMap().get(info.getColumnId()).build();
                Type type = TableLoader.getType(info.getTypeName());
                computeColumnStats(cStoreColumnReader, info.getColumnId(), type).ifPresent(list::add);
            }
            return list.build();
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to read file: " + file, e);
        }
    }

    private List<ColumnInfo> getColumnInfo(OrcReader reader)
    {
        // support for legacy files without metadata
        return getColumnInfoFromOrcColumnTypes(reader.getColumnNames(), reader.getFooter().getTypes());
    }

    private List<ColumnInfo> getColumnInfoFromOrcColumnTypes(List<String> orcColumnNames, List<OrcType> orcColumnTypes)
    {
        Type rowType = getType(orcColumnTypes, 0);
        if (orcColumnNames.size() != rowType.getTypeParameters().size()) {
            throw new PrestoException(RAPTOR_ERROR, "Column names and types do not match");
        }

        ImmutableList.Builder<ColumnInfo> list = ImmutableList.builder();
        for (int i = 0; i < orcColumnNames.size(); i++) {
            list.add(new ColumnInfo(Long.parseLong(orcColumnNames.get(i)), rowType.getTypeParameters().get(i)));
        }
        return list.build();
    }

    private Type getType(List<OrcType> types, int index)
    {
        OrcType type = types.get(index);
        switch (type.getOrcTypeKind()) {
            case BOOLEAN:
                return BOOLEAN;
            case LONG:
                return BIGINT;
            case DOUBLE:
                return DOUBLE;
            case STRING:
                return createUnboundedVarcharType();
            case VARCHAR:
                return createVarcharType(type.getLength().get());
            case CHAR:
                return createCharType(type.getLength().get());
            case BINARY:
                return VARBINARY;
            case DECIMAL:
                return DecimalType.createDecimalType(type.getPrecision().get(), type.getScale().get());
            case LIST:
                TypeSignature elementType = getType(types, type.getFieldTypeIndex(0)).getTypeSignature();
                return typeManager.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(TypeSignatureParameter.of(elementType)));
            case MAP:
                TypeSignature keyType = getType(types, type.getFieldTypeIndex(0)).getTypeSignature();
                TypeSignature valueType = getType(types, type.getFieldTypeIndex(1)).getTypeSignature();
                return typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(TypeSignatureParameter.of(keyType), TypeSignatureParameter.of(valueType)));
            case STRUCT:
                List<String> fieldNames = type.getFieldNames();
                ImmutableList.Builder<TypeSignatureParameter> fieldTypes = ImmutableList.builder();
                for (int i = 0; i < type.getFieldCount(); i++) {
                    fieldTypes.add(TypeSignatureParameter.of(new NamedTypeSignature(
                            Optional.of(new RowFieldName(fieldNames.get(i), false)),
                            getType(types, type.getFieldTypeIndex(i)).getTypeSignature())));
                }
                return typeManager.getParameterizedType(StandardTypes.ROW, fieldTypes.build());
        }
        throw new PrestoException(RAPTOR_ERROR, "Unhandled ORC type: " + type);
    }

    static Type toOrcFileType(Type raptorType, TypeManager typeManager)
    {
        // TIMESTAMPS are stored as BIGINT to void the poor encoding in ORC
        if (raptorType == TimestampType.TIMESTAMP) {
            return BIGINT;
        }
        if (raptorType instanceof ArrayType) {
            Type elementType = toOrcFileType(((ArrayType) raptorType).getElementType(), typeManager);
            return new ArrayType(elementType);
        }
        if (raptorType instanceof MapType) {
            TypeSignature keyType = toOrcFileType(((MapType) raptorType).getKeyType(), typeManager).getTypeSignature();
            TypeSignature valueType = toOrcFileType(((MapType) raptorType).getValueType(), typeManager).getTypeSignature();
            return typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(TypeSignatureParameter.of(keyType), TypeSignatureParameter.of(valueType)));
        }
        if (raptorType instanceof RowType) {
            List<RowType.Field> fields = ((RowType) raptorType).getFields().stream()
                    .map(field -> new RowType.Field(field.getName(), toOrcFileType(field.getType(), typeManager)))
                    .collect(toImmutableList());
            return RowType.from(fields);
        }
        return raptorType;
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
                    throw new PrestoException(RAPTOR_ERROR, "Failed to close writer", e);
                }

                shardRecorder.recordCreatedShard(transactionId, shardUuid);

                Path stagingFile = storageService.getStagingFile(shardUuid);
                futures.add(backupManager.submit(shardUuid, stagingFile));

                Set<String> nodes = ImmutableSet.of(nodeId);
                long rowCount = writer.getRowCount();
                long uncompressedSize = writer.getUncompressedSize();

                shards.add(createShardInfo(fileSystem, shardUuid, bucketNumber, stagingFile, nodes, rowCount, uncompressedSize));

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
                        throw new PrestoException(RAPTOR_ERROR, "Failed to close writer", e);
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
                    sink = orcDataEnvironment.createOrcDataSink(fileSystem, stagingFile);
                }
                catch (IOException e) {
                    throw new PrestoException(RAPTOR_ERROR, format("Failed to create staging file %s", stagingFile), e);
                }
                writer = new CStoreFileWriter(columnIds, columnTypes, stagingDirectory, sink, orcOptimizedWriterStage.equals(ENABLED_AND_VALIDATED), stats, typeManager, compression);
            }
        }
    }

    private static void closeQuietly(Closeable closeable)
    {
        try {
            closeable.close();
        }
        catch (IOException ignored) {
        }
    }

    private static Type getType(String base)
    {
        switch (base) {
            case "integer":
            case "int":
                return IntegerType.INTEGER;
            case "bigint":
            case "long":
                return BigintType.BIGINT;
            case "double":
                return DoubleType.DOUBLE;
            case "varchar":
            case "string":
                return VarcharType.VARCHAR;
            default:
        }
        throw new UnsupportedOperationException();
    }
}
