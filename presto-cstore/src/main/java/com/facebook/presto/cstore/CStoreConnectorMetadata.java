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
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.cstore.manage.CStoreDatabase;
import org.apache.cstore.meta.ColumnMeta;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class CStoreConnectorMetadata
        implements ConnectorMetadata
{
    private final String connectorId;
    private final CStoreDatabase database;

    @Inject
    public CStoreConnectorMetadata(String connectorId, CStoreDatabase database)
    {
        this.connectorId = connectorId;
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
        List<ColumnMeta> columnMetaList = database.getColumn(tableName.getSchemaName(), tableName.getTableName());
        List<ColumnMetadata> columns = columnMetaList.stream()
                .map(columnMeta -> new ColumnMetadata(columnMeta.getName(), convertColumnType(columnMeta)))
                .collect(Collectors.toList());
        return new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Map<String, ColumnHandle> columnHandleMap = new HashMap<>();
        CStoreTableHandle table = (CStoreTableHandle) tableHandle;
        SchemaTableName tableName = new SchemaTableName(table.getSchema(), table.getTable());
        List<ColumnMeta> columnMetaList = database.getColumn(tableName.getSchemaName(), tableName.getTableName());
        for (int i = 0; i < columnMetaList.size(); i++) {
            ColumnMeta columnMeta = columnMetaList.get(i);
            CStoreColumnHandle columnMetadata = new CStoreColumnHandle(connectorId, columnMeta.getName(), convertColumnType(columnMeta), i);
            columnHandleMap.put(columnMetadata.getColumnName(), columnMetadata);
        }
        return columnHandleMap;
    }

    private static Type convertColumnType(ColumnMeta columnMeta)
    {
        switch (columnMeta.getTypeName()) {
            case "int":
                return IntegerType.INTEGER;
            case "long":
                return BigintType.BIGINT;
            case "string":
                return CharType.createCharType(CharType.MAX_LENGTH);
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
        throw new UnsupportedOperationException();
    }
}
