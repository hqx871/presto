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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cstore.metadata.TableIndex;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class CStoreIndexHandle
        implements ConnectorIndexHandle
{
    private final String connectorId;
    private final long indexId;
    private final long tableId;
    private final long[] columnIds;
    private final String indexType;
    private final TupleDomain<CStoreColumnHandle> tupleDomain;

    @JsonCreator
    public CStoreIndexHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("indexId") long indexId,
            @JsonProperty("tableId") long tableId,
            @JsonProperty("columnIds") long[] columnIds,
            @JsonProperty("indexType") String indexType,
            @JsonProperty("tupleDomain") TupleDomain<CStoreColumnHandle> tupleDomain)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.indexId = indexId;
        this.tableId = tableId;
        this.columnIds = columnIds;
        this.indexType = indexType;
        this.tupleDomain = tupleDomain;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public long getIndexId()
    {
        return indexId;
    }

    @JsonProperty
    public long getTableId()
    {
        return tableId;
    }

    @JsonProperty
    public long[] getColumnIds()
    {
        return columnIds;
    }

    @JsonProperty
    public String getIndexType()
    {
        return indexType;
    }

    @JsonProperty
    public TupleDomain<CStoreColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + tableId + ":" + indexId + ":" + columnIds + ":" + indexType;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        CStoreIndexHandle other = (CStoreIndexHandle) obj;
        return Objects.equals(this.indexId, other.indexId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(indexId);
    }

    public static CStoreIndexHandle from(String connectorId, TableIndex tableIndex, TupleDomain<CStoreColumnHandle> tupleDomain)
    {
        return new CStoreIndexHandle(connectorId, tableIndex.getIndexId(), tableIndex.getTableId(), tableIndex.getColumnIds(), tableIndex.getIndexType(), tupleDomain);
    }
}
