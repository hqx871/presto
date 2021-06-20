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
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.cstore.metadata.ColumnInfo;
import com.facebook.presto.cstore.metadata.Distribution;
import com.facebook.presto.cstore.metadata.MetadataDao;
import com.facebook.presto.cstore.metadata.ShardDelta;
import com.facebook.presto.cstore.metadata.ShardInfo;
import com.facebook.presto.cstore.metadata.ShardManager;
import com.facebook.presto.cstore.metadata.Table;
import com.facebook.presto.cstore.metadata.TableColumn;
import com.facebook.presto.cstore.metadata.TableIndex;
import com.facebook.presto.cstore.metadata.ViewResult;
import com.facebook.presto.cstore.storage.StorageTypeConverter;
import com.facebook.presto.cstore.systemtables.ColumnRangesSystemTable;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorTablePartitioning;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.IndexMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import io.airlift.slice.Slice;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.cstore.CStoreBucketFunction.validateBucketType;
import static com.facebook.presto.cstore.CStoreColumnHandle.BUCKET_NUMBER_COLUMN_NAME;
import static com.facebook.presto.cstore.CStoreColumnHandle.SHARD_UUID_COLUMN_NAME;
import static com.facebook.presto.cstore.CStoreColumnHandle.SHARD_UUID_COLUMN_TYPE;
import static com.facebook.presto.cstore.CStoreColumnHandle.bucketNumberColumnHandle;
import static com.facebook.presto.cstore.CStoreColumnHandle.isHiddenColumn;
import static com.facebook.presto.cstore.CStoreColumnHandle.shardRowIdHandle;
import static com.facebook.presto.cstore.CStoreColumnHandle.shardUuidColumnHandle;
import static com.facebook.presto.cstore.CStoreErrorCode.CSTORE_ERROR;
import static com.facebook.presto.cstore.CStoreSessionProperties.getExternalBatchId;
import static com.facebook.presto.cstore.CStoreSessionProperties.getOneSplitPerBucketThreshold;
import static com.facebook.presto.cstore.CStoreTableProperties.BUCKETED_ON_PROPERTY;
import static com.facebook.presto.cstore.CStoreTableProperties.BUCKET_COUNT_PROPERTY;
import static com.facebook.presto.cstore.CStoreTableProperties.DISTRIBUTION_NAME_PROPERTY;
import static com.facebook.presto.cstore.CStoreTableProperties.ORDERING_PROPERTY;
import static com.facebook.presto.cstore.CStoreTableProperties.ORGANIZED_PROPERTY;
import static com.facebook.presto.cstore.CStoreTableProperties.TEMPORAL_COLUMN_PROPERTY;
import static com.facebook.presto.cstore.CStoreTableProperties.getBucketColumns;
import static com.facebook.presto.cstore.CStoreTableProperties.getBucketCount;
import static com.facebook.presto.cstore.CStoreTableProperties.getDistributionName;
import static com.facebook.presto.cstore.CStoreTableProperties.getSortColumns;
import static com.facebook.presto.cstore.CStoreTableProperties.getTemporalColumn;
import static com.facebook.presto.cstore.CStoreTableProperties.isOrganized;
import static com.facebook.presto.cstore.systemtables.ColumnRangesSystemTable.getSourceTable;
import static com.facebook.presto.cstore.util.DatabaseUtil.daoTransaction;
import static com.facebook.presto.cstore.util.DatabaseUtil.onDemandDao;
import static com.facebook.presto.cstore.util.DatabaseUtil.runIgnoringConstraintViolation;
import static com.facebook.presto.cstore.util.DatabaseUtil.runTransaction;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

public class CStoreMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(CStoreMetadata.class);

    private static final JsonCodec<ShardInfo> SHARD_INFO_CODEC = jsonCodec(ShardInfo.class);
    private static final JsonCodec<ShardDelta> SHARD_DELTA_CODEC = jsonCodec(ShardDelta.class);

    private final IDBI dbi;
    private final MetadataDao dao;
    private final ShardManager shardManager;
    private final TypeManager typeManager;
    private final String connectorId;
    private final LongConsumer beginDeleteForTableId;

    private final AtomicReference<Long> currentTransactionId = new AtomicReference<>();

    public CStoreMetadata(String connectorId, IDBI dbi, ShardManager shardManager, TypeManager typeManager)
    {
        this(connectorId, dbi, shardManager, typeManager, tableId -> {});
    }

    public CStoreMetadata(
            String connectorId,
            IDBI dbi,
            ShardManager shardManager,
            TypeManager typeManager,
            LongConsumer beginDeleteForTableId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.dbi = requireNonNull(dbi, "dbi is null");
        this.dao = onDemandDao(dbi, MetadataDao.class);
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.beginDeleteForTableId = requireNonNull(beginDeleteForTableId, "beginDeleteForTableId is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return dao.listSchemaNames();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return getTableHandle(tableName);
    }

    private CStoreTableHandle getTableHandle(SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        Table table = dao.getTableInformation(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }
        List<CStoreColumnHandle> columnHandles = getColumnHandles(table.getTableId(), table.getDistributionId().isPresent());
        //List<TableColumn> tableColumns = dao.listTableColumns(table.getTableId());
        //checkArgument(!tableColumns.isEmpty(), "Table %s does not have any columns", tableName);

        return new CStoreTableHandle(
                connectorId,
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.getTableId(),
                table.getDistributionId(),
                table.getDistributionName(),
                table.getBucketCount(),
                table.isOrganized(),
                OptionalLong.empty(),
                columnHandles,
                false,
                null);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return getSourceTable(tableName)
                .map(this::getTableHandle)
                .map(handle -> new ColumnRangesSystemTable(handle, dbi));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        CStoreTableHandle handle = (CStoreTableHandle) tableHandle;
        SchemaTableName tableName = new SchemaTableName(handle.getSchemaName(), handle.getTableName());
        List<TableColumn> tableColumns = dao.listTableColumns(handle.getTableId());
        if (tableColumns.isEmpty()) {
            throw new TableNotFoundException(tableName);
        }

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        SortedMap<Integer, String> bucketing = new TreeMap<>();
        SortedMap<Integer, String> ordering = new TreeMap<>();

        for (TableColumn column : tableColumns) {
            if (column.isTemporal()) {
                properties.put(TEMPORAL_COLUMN_PROPERTY, column.getColumnName());
            }
            column.getBucketOrdinal().ifPresent(bucketOrdinal -> bucketing.put(bucketOrdinal, column.getColumnName()));
            column.getSortOrdinal().ifPresent(sortOrdinal -> ordering.put(sortOrdinal, column.getColumnName()));
        }

        if (!bucketing.isEmpty()) {
            properties.put(BUCKETED_ON_PROPERTY, ImmutableList.copyOf(bucketing.values()));
        }
        if (!ordering.isEmpty()) {
            properties.put(ORDERING_PROPERTY, ImmutableList.copyOf(ordering.values()));
        }

        handle.getBucketCount().ifPresent(bucketCount -> properties.put(BUCKET_COUNT_PROPERTY, bucketCount));
        handle.getDistributionName().ifPresent(distributionName -> properties.put(DISTRIBUTION_NAME_PROPERTY, distributionName));
        // Only display organization and table_supports_delta_delete property if set
        if (handle.isOrganized()) {
            properties.put(ORGANIZED_PROPERTY, true);
        }

        List<ColumnMetadata> columns = tableColumns.stream()
                .map(TableColumn::toColumnMetadata)
                .collect(toCollection(ArrayList::new));

        columns.add(hiddenColumn(SHARD_UUID_COLUMN_NAME, SHARD_UUID_COLUMN_TYPE));

        if (handle.isBucketed()) {
            columns.add(hiddenColumn(BUCKET_NUMBER_COLUMN_NAME, INTEGER));
        }

        properties.putAll(getExtraProperties(handle.getTableId()));

        return new ConnectorTableMetadata(tableName, columns, properties.build());
    }

    protected Map<String, Object> getExtraProperties(long tableid)
    {
        return ImmutableMap.of();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, @Nullable String schemaNameOrNull)
    {
        return dao.listTables(schemaNameOrNull);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        CStoreTableHandle cStoreTableHandle = (CStoreTableHandle) tableHandle;
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        List<CStoreColumnHandle> columnHandles = cStoreTableHandle.getColumns();
        if (columnHandles == null) {
            columnHandles = getColumnHandles(cStoreTableHandle.getTableId(), cStoreTableHandle.isBucketed());
        }
        for (CStoreColumnHandle tableColumn : columnHandles) {
            builder.put(tableColumn.getColumnName(), tableColumn);
        }
        return builder.build();
    }

    @Override
    public Map<String, ConnectorIndexHandle> getIndexHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        CStoreTableHandle raptorTableHandle = (CStoreTableHandle) tableHandle;
        ImmutableMap.Builder<String, ConnectorIndexHandle> builder = ImmutableMap.builder();
        for (TableIndex tableColumn : dao.listTableIndexes(raptorTableHandle.getTableId())) {
            builder.put(tableColumn.getName(), CStoreIndexHandle.from(connectorId, tableColumn));
        }
        return builder.build();
    }

    private List<CStoreColumnHandle> getColumnHandles(long tableId, boolean bucketed)
    {
        ImmutableList.Builder<CStoreColumnHandle> builder = ImmutableList.builder();
        for (TableColumn tableColumn : dao.listTableColumns(tableId)) {
            builder.add(getCStoreColumnHandle(tableColumn));
        }

        CStoreColumnHandle uuidColumn = shardUuidColumnHandle(connectorId);
        builder.add(uuidColumn);

        if (bucketed) {
            CStoreColumnHandle bucketNumberColumn = bucketNumberColumnHandle(connectorId);
            builder.add(bucketNumberColumn);
        }

        return builder.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        CStoreColumnHandle column = (CStoreColumnHandle) columnHandle;

        if (isHiddenColumn(column.getColumnId())) {
            return hiddenColumn(column.getColumnName(), column.getColumnType());
        }

        return new ColumnMetadata(column.getColumnName(), column.getColumnType());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        ImmutableListMultimap.Builder<SchemaTableName, ColumnMetadata> columns = ImmutableListMultimap.builder();
        for (TableColumn tableColumn : dao.listTableColumns(prefix.getSchemaName(), prefix.getTableName())) {
            ColumnMetadata columnMetadata = new ColumnMetadata(tableColumn.getColumnName(), tableColumn.getDataType());
            columns.put(tableColumn.getTable(), columnMetadata);
        }
        return Multimaps.asMap(columns.build());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        CStoreTableHandle handle = (CStoreTableHandle) table;
        ConnectorTableLayout layout = getTableLayout(session, handle, constraint.getSummary());
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        CStoreTableLayoutHandle raptorHandle = (CStoreTableLayoutHandle) handle;
        return getTableLayout(session, raptorHandle.getTable(), raptorHandle.getConstraint());
    }

    private ConnectorTableLayout getTableLayout(ConnectorSession session, CStoreTableHandle handle, TupleDomain<ColumnHandle> constraint)
    {
        if (!handle.getDistributionId().isPresent()) {
            return new ConnectorTableLayout(new CStoreTableLayoutHandle(handle, constraint, Optional.empty(), null));
        }

        List<CStoreColumnHandle> bucketColumnHandles = getBucketColumnHandles(handle.getTableId());

        CStorePartitioningHandle partitioning = getPartitioningHandle(handle.getDistributionId().getAsLong());

        boolean oneSplitPerBucket = handle.getBucketCount().getAsInt() >= getOneSplitPerBucketThreshold(session);

        return new ConnectorTableLayout(
                new CStoreTableLayoutHandle(handle, constraint, Optional.of(partitioning), null),
                Optional.empty(),
                TupleDomain.all(),
                Optional.of(new ConnectorTablePartitioning(
                        partitioning,
                        ImmutableList.copyOf(bucketColumnHandles))),
                oneSplitPerBucket ? Optional.of(ImmutableSet.copyOf(bucketColumnHandles)) : Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata metadata)
    {
        ImmutableMap.Builder<String, CStoreColumnHandle> map = ImmutableMap.builder();
        long columnId = 1;
        StorageTypeConverter storageTypeConverter = new StorageTypeConverter(typeManager);
        for (ColumnMetadata column : metadata.getColumns()) {
            checkState(storageTypeConverter.toStorageType(column.getType()) != null, "storage type cannot be null");
            map.put(column.getName(), new CStoreColumnHandle(connectorId, column.getName(), columnId, column.getType()));
            columnId++;
        }

        Optional<DistributionInfo> distribution = getOrCreateDistribution(map.build(), metadata.getProperties());
        if (!distribution.isPresent()) {
            return Optional.empty();
        }

        List<String> partitionColumns = distribution.get().getBucketColumns().stream()
                .map(CStoreColumnHandle::getColumnName)
                .collect(toList());

        ConnectorPartitioningHandle partitioning = getPartitioningHandle(distribution.get().getDistributionId());
        return Optional.of(new ConnectorNewTableLayout(partitioning, partitionColumns));
    }

    private CStorePartitioningHandle getPartitioningHandle(long distributionId)
    {
        return new CStorePartitioningHandle(distributionId, shardManager.getBucketAssignments(distributionId));
    }

    private Optional<DistributionInfo> getOrCreateDistribution(Map<String, CStoreColumnHandle> columnHandleMap, Map<String, Object> properties)
    {
        OptionalInt bucketCount = getBucketCount(properties);
        List<CStoreColumnHandle> bucketColumnHandles = getBucketColumnHandles(getBucketColumns(properties), columnHandleMap);

        if (bucketCount.isPresent() && bucketColumnHandles.isEmpty()) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("Must specify '%s' along with '%s'", BUCKETED_ON_PROPERTY, BUCKET_COUNT_PROPERTY));
        }
        if (!bucketCount.isPresent() && !bucketColumnHandles.isEmpty()) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("Must specify '%s' along with '%s'", BUCKET_COUNT_PROPERTY, BUCKETED_ON_PROPERTY));
        }
        ImmutableList.Builder<Type> bucketColumnTypes = ImmutableList.builder();
        for (CStoreColumnHandle column : bucketColumnHandles) {
            validateBucketType(column.getColumnType());
            bucketColumnTypes.add(column.getColumnType());
        }

        long distributionId;
        String distributionName = getDistributionName(properties);
        if (distributionName != null) {
            if (bucketColumnHandles.isEmpty()) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, format("Must specify '%s' along with '%s'", BUCKETED_ON_PROPERTY, DISTRIBUTION_NAME_PROPERTY));
            }

            Distribution distribution = dao.getDistribution(distributionName);
            if (distribution == null) {
                if (!bucketCount.isPresent()) {
                    throw new PrestoException(INVALID_TABLE_PROPERTY, "Distribution does not exist and bucket count is not specified");
                }
                distribution = getOrCreateDistribution(distributionName, bucketColumnTypes.build(), bucketCount.getAsInt());
            }
            distributionId = distribution.getId();

            if (bucketCount.isPresent() && (distribution.getBucketCount() != bucketCount.getAsInt())) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "Bucket count must match distribution");
            }
            if (!distribution.getColumnTypes().equals(bucketColumnTypes.build())) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "Bucket column types must match distribution");
            }
        }
        else if (bucketCount.isPresent()) {
            String types = Distribution.serializeColumnTypes(bucketColumnTypes.build());
            distributionId = dao.insertDistribution(null, types, bucketCount.getAsInt());
        }
        else {
            return Optional.empty();
        }

        shardManager.createBuckets(distributionId, bucketCount.getAsInt());

        return Optional.of(new DistributionInfo(distributionId, bucketCount.getAsInt(), bucketColumnHandles));
    }

    private Distribution getOrCreateDistribution(String name, List<Type> columnTypes, int bucketCount)
    {
        String types = Distribution.serializeColumnTypes(columnTypes);
        runIgnoringConstraintViolation(() -> dao.insertDistribution(name, types, bucketCount));

        Distribution distribution = dao.getDistribution(name);
        if (distribution == null) {
            throw new PrestoException(CSTORE_ERROR, "Distribution does not exist after insert");
        }
        return distribution;
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        Optional<ConnectorNewTableLayout> layout = getNewTableLayout(session, tableMetadata);
        finishCreateTable(session, beginCreateTable(session, tableMetadata, layout), ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        CStoreTableHandle raptorHandle = (CStoreTableHandle) tableHandle;
        shardManager.dropTable(raptorHandle.getTableId());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        CStoreTableHandle table = (CStoreTableHandle) tableHandle;
        runTransaction(dbi, (handle, status) -> {
            MetadataDao dao = handle.attach(MetadataDao.class);
            dao.renameTable(table.getTableId(), newTableName.getSchemaName(), newTableName.getTableName());
            return null;
        });
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        CStoreTableHandle table = (CStoreTableHandle) tableHandle;

        // Always add new columns to the end.
        List<TableColumn> existingColumns = dao.listTableColumns(table.getSchemaName(), table.getTableName());
        TableColumn lastColumn = existingColumns.get(existingColumns.size() - 1);
        long columnId = lastColumn.getColumnId() + 1;
        int ordinalPosition = lastColumn.getOrdinalPosition() + 1;

        StorageTypeConverter storageTypeConverter = new StorageTypeConverter(typeManager);
        checkState(storageTypeConverter.toStorageType(column.getType()) != null, "storage type cannot be null");
        String type = column.getType().getTypeSignature().toString();
        daoTransaction(dbi, MetadataDao.class, dao -> {
            dao.insertColumn(table.getTableId(), columnId, column.getName(), ordinalPosition, type, null, null);
            dao.updateTableVersion(table.getTableId(), session.getStartTime());
        });

        shardManager.addColumn(table.getTableId(), new ColumnInfo(columnId, column.getType()));
    }

    @Override
    public void addIndex(ConnectorSession session, ConnectorTableHandle tableHandle, IndexMetadata index)
    {
        //todo rerun old shard to build index?
        CStoreTableHandle table = (CStoreTableHandle) tableHandle;

        List<TableColumn> columns = dao.listTableColumns(table.getSchemaName(), table.getTableName());
        Map<String, TableColumn> columnMap = Maps.uniqueIndex(columns, TableColumn::getColumnName);

        daoTransaction(dbi, MetadataDao.class, dao -> {
            List<Long> columnIds = index.getColumns().stream().map(column -> columnMap.get(column).getColumnId()).collect(toList());
            String columnIdString = Joiner.on(",").join(columnIds);
            dao.insertTableIndex(index.getName(), table.getTableId(), columnIdString, index.getUsing().orElse("bitmap"));
            dao.updateTableVersion(table.getTableId(), session.getStartTime());
        });
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        CStoreTableHandle table = (CStoreTableHandle) tableHandle;
        CStoreColumnHandle sourceColumn = (CStoreColumnHandle) source;
        daoTransaction(dbi, MetadataDao.class, dao -> {
            dao.renameColumn(table.getTableId(), sourceColumn.getColumnId(), target);
            dao.updateTableVersion(table.getTableId(), session.getStartTime());
        });
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        CStoreTableHandle table = (CStoreTableHandle) tableHandle;
        CStoreColumnHandle raptorColumn = (CStoreColumnHandle) column;

        List<TableColumn> existingColumns = dao.listTableColumns(table.getSchemaName(), table.getTableName());
        if (existingColumns.size() <= 1) {
            throw new PrestoException(NOT_SUPPORTED, "Cannot drop the only column in a table");
        }
        long maxColumnId = existingColumns.stream().mapToLong(TableColumn::getColumnId).max().getAsLong();
        if (raptorColumn.getColumnId() == maxColumnId) {
            throw new PrestoException(NOT_SUPPORTED, "Cannot drop the column which has the largest column ID in the table");
        }

        if (getBucketColumnHandles(table.getTableId()).contains(column)) {
            throw new PrestoException(NOT_SUPPORTED, "Cannot drop bucket columns");
        }

        Optional.ofNullable(dao.getTemporalColumnId(table.getTableId())).ifPresent(tempColumnId -> {
            if (raptorColumn.getColumnId() == tempColumnId) {
                throw new PrestoException(NOT_SUPPORTED, "Cannot drop the temporal column");
            }
        });

        if (getSortColumnHandles(table.getTableId()).contains(raptorColumn)) {
            throw new PrestoException(NOT_SUPPORTED, "Cannot drop sort columns");
        }

        daoTransaction(dbi, MetadataDao.class, dao -> {
            dao.dropColumn(table.getTableId(), raptorColumn.getColumnId());
            //todo drop table index
            dao.updateTableVersion(table.getTableId(), session.getStartTime());
        });

        // TODO: drop column from index table
    }

    @Override
    public void dropIndex(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorIndexHandle index)
    {
        CStoreTableHandle table = (CStoreTableHandle) tableHandle;
        CStoreIndexHandle cStoreIndexHandle = (CStoreIndexHandle) index;

        daoTransaction(dbi, MetadataDao.class, dao -> {
            dao.dropIndex(table.getTableId(), cStoreIndexHandle.getIndexId());
            dao.updateTableVersion(table.getTableId(), session.getStartTime());
        });
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        if (viewExists(session, tableMetadata.getTable())) {
            throw new PrestoException(ALREADY_EXISTS, "View already exists: " + tableMetadata.getTable());
        }

        runExtraEligibilityCheck(session, tableMetadata);

        Optional<CStorePartitioningHandle> partitioning = layout
                .map(ConnectorNewTableLayout::getPartitioning)
                .map(CStorePartitioningHandle.class::cast);

        ImmutableList.Builder<CStoreColumnHandle> columnHandles = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();

        long columnId = 1;
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            columnHandles.add(new CStoreColumnHandle(connectorId, column.getName(), columnId, column.getType()));
            columnTypes.add(column.getType());
            columnId++;
        }

        Map<String, CStoreColumnHandle> columnHandleMap = Maps.uniqueIndex(columnHandles.build(), CStoreColumnHandle::getColumnName);
        List<CStoreIndexHandle> indexHandles = new ArrayList<>();
        int indexId = 1;
        for (IndexMetadata indexMetadata : tableMetadata.getIndexes()) {
            long[] columnIds = indexMetadata.getColumns().stream().mapToLong(col -> columnHandleMap.get(col).getColumnId()).toArray();
            indexHandles.add(new CStoreIndexHandle(connectorId, indexMetadata.getName(), indexId, columnIds, indexMetadata.getUsing().orElse("bitmap")));
            indexId++;
        }

        List<CStoreColumnHandle> sortColumnHandles = getSortColumnHandles(getSortColumns(tableMetadata.getProperties()), columnHandleMap);
        Optional<CStoreColumnHandle> temporalColumnHandle = getTemporalColumnHandle(getTemporalColumn(tableMetadata.getProperties()), columnHandleMap);

        if (temporalColumnHandle.isPresent()) {
            CStoreColumnHandle column = temporalColumnHandle.get();
            if (!column.getColumnType().equals(TIMESTAMP) && !column.getColumnType().equals(DATE)) {
                throw new PrestoException(NOT_SUPPORTED, "Temporal column must be of type timestamp or date: " + column.getColumnName());
            }
        }

        boolean organized = isOrganized(tableMetadata.getProperties());
        if (organized) {
            if (temporalColumnHandle.isPresent()) {
                throw new PrestoException(NOT_SUPPORTED, "Table with temporal columns cannot be organized");
            }
            if (sortColumnHandles.isEmpty()) {
                throw new PrestoException(NOT_SUPPORTED, "Table organization requires an ordering");
            }
        }

        long transactionId = shardManager.beginTransaction();

        setTransactionId(transactionId);

        Optional<DistributionInfo> distribution = partitioning.map(handle ->
                getDistributionInfo(handle.getDistributionId(), columnHandleMap, tableMetadata.getProperties()));

        return new CStoreOutputTableHandle(
                connectorId,
                transactionId,
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                columnHandles.build(),
                columnTypes.build(),
                sortColumnHandles,
                nCopies(sortColumnHandles.size(), ASC_NULLS_FIRST),
                temporalColumnHandle,
                distribution.map(info -> OptionalLong.of(info.getDistributionId())).orElse(OptionalLong.empty()),
                distribution.map(info -> OptionalInt.of(info.getBucketCount())).orElse(OptionalInt.empty()),
                distribution.map(DistributionInfo::getBucketColumns).orElse(ImmutableList.of()),
                organized,
                tableMetadata.getProperties().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString())),
                indexHandles);
    }

    private DistributionInfo getDistributionInfo(long distributionId, Map<String, CStoreColumnHandle> columnHandleMap, Map<String, Object> properties)
    {
        Distribution distribution = dao.getDistribution(distributionId);
        if (distribution == null) {
            throw new PrestoException(CSTORE_ERROR, "Distribution ID does not exist: " + distributionId);
        }
        List<CStoreColumnHandle> bucketColumnHandles = getBucketColumnHandles(getBucketColumns(properties), columnHandleMap);
        return new DistributionInfo(distributionId, distribution.getBucketCount(), bucketColumnHandles);
    }

    private static Optional<CStoreColumnHandle> getTemporalColumnHandle(String temporalColumn, Map<String, CStoreColumnHandle> columnHandleMap)
    {
        if (temporalColumn == null) {
            return Optional.empty();
        }

        CStoreColumnHandle handle = columnHandleMap.get(temporalColumn);
        if (handle == null) {
            throw new PrestoException(NOT_FOUND, "Temporal column does not exist: " + temporalColumn);
        }
        return Optional.of(handle);
    }

    private static List<CStoreColumnHandle> getSortColumnHandles(List<String> sortColumns, Map<String, CStoreColumnHandle> columnHandleMap)
    {
        ImmutableList.Builder<CStoreColumnHandle> columnHandles = ImmutableList.builder();
        for (String column : sortColumns) {
            if (!columnHandleMap.containsKey(column)) {
                throw new PrestoException(NOT_FOUND, "Ordering column does not exist: " + column);
            }
            columnHandles.add(columnHandleMap.get(column));
        }
        return columnHandles.build();
    }

    private static List<CStoreColumnHandle> getBucketColumnHandles(List<String> bucketColumns, Map<String, CStoreColumnHandle> columnHandleMap)
    {
        ImmutableList.Builder<CStoreColumnHandle> columnHandles = ImmutableList.builder();
        for (String column : bucketColumns) {
            if (!columnHandleMap.containsKey(column)) {
                throw new PrestoException(NOT_FOUND, "Bucketing column does not exist: " + column);
            }
            columnHandles.add(columnHandleMap.get(column));
        }
        return columnHandles.build();
    }

    protected void runExtraEligibilityCheck(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        // no-op
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        CStoreOutputTableHandle table = (CStoreOutputTableHandle) outputTableHandle;
        long transactionId = table.getTransactionId();
        long updateTime = session.getStartTime();

        long newTableId = runTransaction(dbi, (dbiHandle, status) -> {
            MetadataDao dao = dbiHandle.attach(MetadataDao.class);

            Long distributionId = table.getDistributionId().isPresent() ? table.getDistributionId().getAsLong() : null;
            // TODO: update default value of organization_enabled to true
            long tableId = dao.insertTable(table.getSchemaName(), table.getTableName(), true, table.isOrganized(), distributionId, updateTime, false);

            runExtraCreateTableStatement(tableId, dbiHandle, table.getProperties());

            List<CStoreColumnHandle> sortColumnHandles = table.getSortColumnHandles();
            List<CStoreColumnHandle> bucketColumnHandles = table.getBucketColumnHandles();

            for (int i = 0; i < table.getColumnTypes().size(); i++) {
                CStoreColumnHandle column = table.getColumnHandles().get(i);

                int columnId = i + 1;
                String type = table.getColumnTypes().get(i).getTypeSignature().toString();
                Integer sortPosition = sortColumnHandles.contains(column) ? sortColumnHandles.indexOf(column) : null;
                Integer bucketPosition = bucketColumnHandles.contains(column) ? bucketColumnHandles.indexOf(column) : null;

                dao.insertColumn(tableId, columnId, column.getColumnName(), i, type, sortPosition, bucketPosition);
                if (table.getTemporalColumnHandle().isPresent() && table.getTemporalColumnHandle().get().equals(column)) {
                    dao.updateTemporalColumnId(tableId, columnId);
                }
            }

            for (int i = 0; i < table.getIndexHandles().size(); i++) {
                CStoreIndexHandle indexHandle = table.getIndexHandles().get(i);
                String columnIds = Joiner.on(",").join(Arrays.stream(indexHandle.getColumnIds()).boxed().toArray());
                dao.insertTableIndex(indexHandle.getName(), tableId, columnIds, indexHandle.getIndexType().toLowerCase(Locale.getDefault()));
            }

            return tableId;
        });

        List<TableColumn> columns = dao.listTableColumns(newTableId);

        OptionalLong temporalColumnId = table.getTemporalColumnHandle().map(CStoreColumnHandle::getColumnId)
                .map(OptionalLong::of)
                .orElse(OptionalLong.empty());

        // TODO: refactor this to avoid creating an empty table on failure
        shardManager.createTable(newTableId, columns, table.getBucketCount().isPresent(), temporalColumnId, false);
        shardManager.commitShards(transactionId, newTableId, columns, parseFragments(fragments), Optional.empty(), updateTime);

        clearRollback();

        return Optional.empty();
    }

    protected void runExtraCreateTableStatement(long tableId, Handle dbiHandle, Map<String, String> extraProperties)
    {
        // no-op
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        CStoreTableHandle handle = (CStoreTableHandle) tableHandle;
        long tableId = handle.getTableId();

        ImmutableList.Builder<CStoreColumnHandle> columnHandles = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        for (TableColumn column : dao.listTableColumns(tableId)) {
            columnHandles.add(new CStoreColumnHandle(connectorId, column.getColumnName(), column.getColumnId(), column.getDataType()));
            columnTypes.add(column.getDataType());
        }

        long transactionId = shardManager.beginTransaction();

        setTransactionId(transactionId);

        Optional<String> externalBatchId = getExternalBatchId(session);
        List<CStoreColumnHandle> sortColumnHandles = getSortColumnHandles(tableId);
        List<CStoreColumnHandle> bucketColumnHandles = getBucketColumnHandles(tableId);

        Optional<CStoreColumnHandle> temporalColumnHandle = Optional.ofNullable(dao.getTemporalColumnId(tableId))
                .map(temporalColumnId -> getOnlyElement(columnHandles.build().stream()
                        .filter(columnHandle -> columnHandle.getColumnId() == temporalColumnId)
                        .collect(toList())));

        return new CStoreInsertTableHandle(connectorId,
                transactionId,
                tableId,
                columnHandles.build(),
                columnTypes.build(),
                externalBatchId,
                sortColumnHandles,
                nCopies(sortColumnHandles.size(), ASC_NULLS_FIRST),
                handle.getBucketCount(),
                bucketColumnHandles,
                temporalColumnHandle,
                handle.getSchemaName(),
                handle.getTableName());
    }

    private List<CStoreColumnHandle> getSortColumnHandles(long tableId)
    {
        return dao.listSortColumns(tableId).stream()
                .map(this::getCStoreColumnHandle)
                .collect(toList());
    }

    private List<CStoreColumnHandle> getBucketColumnHandles(long tableId)
    {
        return dao.listBucketColumns(tableId).stream()
                .map(this::getCStoreColumnHandle)
                .collect(toList());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        CStoreInsertTableHandle handle = (CStoreInsertTableHandle) insertHandle;
        long transactionId = handle.getTransactionId();
        long tableId = handle.getTableId();
        Optional<String> externalBatchId = handle.getExternalBatchId();
        Set<Long> columnIds = handle.getBucketColumnHandles().stream().map(CStoreColumnHandle::getColumnId).collect(Collectors.toSet());
        List<TableColumn> columns = dao.listTableColumns(tableId).stream().filter(column -> columnIds.contains(column.getColumnId())).collect(toList());
        long updateTime = session.getStartTime();

        Collection<ShardInfo> shards = parseFragments(fragments);
        log.info("Committing insert into tableId %s (queryId: %s, shards: %s, columns: %s)", handle.getTableId(), session.getQueryId(), shards.size(), columns.size());
        shardManager.commitShards(transactionId, tableId, columns, shards, externalBatchId, updateTime);

        clearRollback();

        return Optional.empty();
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return shardRowIdHandle(connectorId);
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        CStoreTableHandle handle = (CStoreTableHandle) tableHandle;
        beginDeleteForTableId.accept(handle.getTableId());
        long transactionId = shardManager.beginTransaction();
        setTransactionId(transactionId);
        handle.setDelete(true);
        handle.setTransactionId(OptionalLong.of(transactionId));
        return handle;
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        CStoreTableHandle table = (CStoreTableHandle) tableHandle;
        long transactionId = table.getTransactionId().getAsLong();
        long tableId = table.getTableId();

        List<TableColumn> columns = dao.listTableColumns(tableId);

        ImmutableSet.Builder<UUID> oldShardUuidsBuilder = ImmutableSet.builder();
        ImmutableList.Builder<ShardInfo> newShardsBuilder = ImmutableList.builder();

        fragments.stream()
                .map(fragment -> SHARD_DELTA_CODEC.fromJson(fragment.getBytes()))
                .forEach(delta -> {
                    oldShardUuidsBuilder.addAll(delta.getOldShardUuids());
                    newShardsBuilder.addAll(delta.getNewShards());
                });

        Set<UUID> oldShardUuids = oldShardUuidsBuilder.build();
        List<ShardInfo> newShards = newShardsBuilder.build();
        OptionalLong updateTime = OptionalLong.of(session.getStartTime());

        log.info("Finishing delete for tableId %s (removed: %s, rewritten: %s)", tableId, oldShardUuids.size() - newShards.size(), newShards.size());
        shardManager.replaceShardUuids(transactionId, tableId, columns, oldShardUuids, newShards, updateTime);

        clearRollback();
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle)
    {
        return false;
    }

    @Override
    public void createView(ConnectorSession session, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        SchemaTableName viewName = viewMetadata.getTable();
        String schemaName = viewName.getSchemaName();
        String tableName = viewName.getTableName();

        if (getTableHandle(viewName) != null) {
            throw new PrestoException(ALREADY_EXISTS, "Table already exists: " + viewName);
        }

        if (replace) {
            daoTransaction(dbi, MetadataDao.class, dao -> {
                dao.dropView(schemaName, tableName);
                dao.insertView(schemaName, tableName, viewData);
            });
            return;
        }

        try {
            dao.insertView(schemaName, tableName, viewData);
        }
        catch (PrestoException e) {
            if (viewExists(session, viewName)) {
                throw new PrestoException(ALREADY_EXISTS, "View already exists: " + viewName);
            }
            throw e;
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        if (!viewExists(session, viewName)) {
            throw new ViewNotFoundException(viewName);
        }
        dao.dropView(viewName.getSchemaName(), viewName.getTableName());
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return dao.listViews(schemaNameOrNull);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> map = ImmutableMap.builder();
        for (ViewResult view : dao.getViews(prefix.getSchemaName(), prefix.getTableName())) {
            map.put(view.getName(), new ConnectorViewDefinition(view.getName(), Optional.empty(), view.getData()));
        }
        return map.build();
    }

    public boolean hasBitmap(long tableId, String columnName)
    {
        Optional<TableColumn> tableColumn = dao.listTableColumns(tableId).stream()
                .filter(column -> Objects.equals(column.getColumnName(), columnName))
                .findAny();
        if (!tableColumn.isPresent()) {
            return false;
        }
        List<TableIndex> indices = dao.listTableIndexes(tableId, "bitmap");
        return indices.stream().anyMatch(index -> index.getColumnIds().length == 1 && index.getColumnIds()[0] == tableColumn.get().getColumnId());
    }

    private boolean viewExists(ConnectorSession session, SchemaTableName viewName)
    {
        return !getViews(session, viewName.toSchemaTablePrefix()).isEmpty();
    }

    private CStoreColumnHandle getCStoreColumnHandle(TableColumn tableColumn)
    {
        return new CStoreColumnHandle(connectorId, tableColumn.getColumnName(), tableColumn.getColumnId(), tableColumn.getDataType());
    }

    private static Collection<ShardInfo> parseFragments(Collection<Slice> fragments)
    {
        return fragments.stream()
                .map(fragment -> SHARD_INFO_CODEC.fromJson(fragment.getBytes()))
                .collect(toList());
    }

    private static ColumnMetadata hiddenColumn(String name, Type type)
    {
        return new ColumnMetadata(name, type, null, true);
    }

    private void setTransactionId(long transactionId)
    {
        checkState(currentTransactionId.compareAndSet(null, transactionId), "current transaction ID already set");
    }

    private void clearRollback()
    {
        currentTransactionId.set(null);
    }

    public void rollback()
    {
        Long transactionId = currentTransactionId.getAndSet(null);
        if (transactionId != null) {
            shardManager.rollbackTransaction(transactionId);
        }
    }

    private static class DistributionInfo
    {
        private final long distributionId;
        private final int bucketCount;
        private final List<CStoreColumnHandle> bucketColumns;

        public DistributionInfo(long distributionId, int bucketCount, List<CStoreColumnHandle> bucketColumns)
        {
            this.distributionId = distributionId;
            this.bucketCount = bucketCount;
            this.bucketColumns = ImmutableList.copyOf(requireNonNull(bucketColumns, "bucketColumns is null"));
        }

        public long getDistributionId()
        {
            return distributionId;
        }

        public int getBucketCount()
        {
            return bucketCount;
        }

        public List<CStoreColumnHandle> getBucketColumns()
        {
            return bucketColumns;
        }
    }
}
