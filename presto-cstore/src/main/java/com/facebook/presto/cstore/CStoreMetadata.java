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

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import org.apache.cstore.CStoreDatabase;
import org.apache.cstore.meta.ColumnMeta;
import org.apache.cstore.meta.DbMeta;
import org.apache.cstore.meta.TableMeta;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class CStoreMetadata
        implements ConnectorMetadata
{
    private final String connectorId;
    private final CStoreDatabase database;

    @Inject
    public CStoreMetadata(CStoreConnectorId connectorId, CStoreDatabase database)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.database = database;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return Lists.newArrayList(database.getDbMetaMap().keySet());
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        //todo remove filter?
        return new CStoreTableHandle(tableName.getSchemaName(), tableName.getTableName(), null);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        CStoreTableHandle storeTableHandle = (CStoreTableHandle) table;
        ConnectorTableLayout tableLayout = new ConnectorTableLayout(new CStoreTableLayoutHandle(storeTableHandle));
        ConnectorTableLayoutResult result = new ConnectorTableLayoutResult(tableLayout, constraint.getSummary());
        return ImmutableList.of(result);
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        CStoreTableHandle tableHandle = (CStoreTableHandle) table;
        SchemaTableName tableName = new SchemaTableName(tableHandle.getSchema(), tableHandle.getTable());
        List<ColumnMetadata> columns = listColumnMetadata(tableName.getSchemaName(), tableName.getTableName());
        return new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Map<String, ColumnHandle> columnHandleMap = new LinkedHashMap<>();
        CStoreTableHandle table = (CStoreTableHandle) tableHandle;
        List<CStoreColumnHandle> columnHandles = listColumnHandles(table);
        for (int i = 0; i < columnHandles.size(); i++) {
            CStoreColumnHandle columnHandle = columnHandles.get(i);
            columnHandleMap.put(columnHandle.getColumnName(), columnHandle);
        }
        return columnHandleMap;
    }

    private List<CStoreColumnHandle> listColumnHandles(CStoreTableHandle table)
    {
        List<CStoreColumnHandle> columnHandles = new ArrayList<>();
        List<ColumnMeta> columnMetaList = database.getColumn(table.getSchema(), table.getTable());
        for (int i = 0; i < columnMetaList.size(); i++) {
            ColumnMeta columnMeta = columnMetaList.get(i);
            CStoreColumnHandle columnMetadata = new CStoreColumnHandle(connectorId, columnMeta.getName(), convertColumnType(columnMeta), i);
            columnHandles.add(columnMetadata);
        }
        return columnHandles;
    }

    private static Type convertColumnType(ColumnMeta columnMeta)
    {
        switch (columnMeta.getTypeName()) {
            case "int":
                return IntegerType.INTEGER;
            case "long":
                return BigintType.BIGINT;
            case "string":
                return VarcharType.VARCHAR;
            case "double":
                return DoubleType.DOUBLE;
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        CStoreColumnHandle storeColumnHandle = (CStoreColumnHandle) columnHandle;
        return new ColumnMetadata(storeColumnHandle.getColumnName(), storeColumnHandle.getColumnType());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        Map<String, DbMeta> dbMetaMap = database.getDbMetaMap();
        Map<SchemaTableName, List<ColumnMetadata>> tables = new LinkedHashMap<>();
        for (DbMeta dbMeta : dbMetaMap.values()) {
            if (prefix.getSchemaName() != null && !dbMeta.getName().startsWith(prefix.getSchemaName())) {
                continue;
            }
            Map<String, TableMeta> tableMetaMap = dbMeta.getTableMap();
            for (TableMeta tableMeta : tableMetaMap.values()) {
                if (prefix.getTableName() != null && !tableMeta.getName().startsWith(prefix.getTableName())) {
                    continue;
                }
                SchemaTableName schemaTableName = new SchemaTableName(dbMeta.getName(), tableMeta.getName());
                List<ColumnMetadata> columns = listColumnMetadata(dbMeta.getName(), tableMeta.getName());
                tables.put(schemaTableName, columns);
            }
        }
        return tables;
    }

    private List<ColumnMetadata> listColumnMetadata(String schema, String table)
    {
        return database.getColumn(schema, table).stream()
                .map(columnMeta -> new ColumnMetadata(columnMeta.getName(), convertColumnType(columnMeta)))
                .collect(Collectors.toList());
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        CStoreTableHandle tableHandle = new CStoreTableHandle(tableMetadata.getTable().getSchemaName(), tableMetadata.getTable().getTableName(), null);
        List<CStoreColumnHandle> columnHandles = new ArrayList<>(tableMetadata.getColumns().size());
        for (int i = 0; i < tableMetadata.getColumns().size(); i++) {
            ColumnMetadata column = tableMetadata.getColumns().get(i);
            CStoreColumnHandle columnHandle = new CStoreColumnHandle(connectorId, column.getName(), column.getType(), i);
            columnHandles.add(columnHandle);
        }
        return new CStoreOutputTableHandle(tableHandle, columnHandles);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        CStoreOutputTableHandle cStoreOutputTableHandle = (CStoreOutputTableHandle) tableHandle;
        return Optional.empty();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        CStoreTableHandle cStoreTableHandle = (CStoreTableHandle) tableHandle;
        return new CStoreInsertTableHandle(cStoreTableHandle, listColumnHandles(cStoreTableHandle));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        CStoreInsertTableHandle cStoreInsertTableHandle = (CStoreInsertTableHandle) insertHandle;
        return Optional.empty();
    }
}
