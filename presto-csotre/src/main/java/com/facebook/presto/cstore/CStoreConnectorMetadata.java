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

import com.facebook.presto.common.type.IntegerType;
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

import javax.inject.Inject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class CStoreConnectorMetadata
        implements ConnectorMetadata
{
    private final String connectorId;

    @Inject
    public CStoreConnectorMetadata(String connectorId)
    {
        this.connectorId = connectorId;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return Arrays.asList("db", "db1", "db2");
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
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
        return new ConnectorTableMetadata(tableName, ImmutableList.of(new ColumnMetadata("col_int", IntegerType.INTEGER)));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Map<String, ColumnHandle> map = new HashMap<>();
        map.put("col_int", new CStoreColumnHandle(connectorId, "col_int", IntegerType.INTEGER, 0));
        return map;
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
        return null;
    }
}
