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

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.facebook.presto.cstore.util.MetadataUtil.checkSchemaName;
import static com.facebook.presto.cstore.util.MetadataUtil.checkTableName;
import static java.util.Objects.requireNonNull;

public class CStoreOutputTableHandle
        implements ConnectorOutputTableHandle
{
    private final String connectorId;
    private final long transactionId;
    private final String schemaName;
    private final String tableName;
    private final List<CStoreColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final List<CStoreColumnHandle> sortColumnHandles;
    private final List<SortOrder> sortOrders;
    private final Optional<CStoreColumnHandle> temporalColumnHandle;
    private final OptionalLong distributionId;
    private final OptionalInt bucketCount;
    private final List<CStoreColumnHandle> bucketColumnHandles;
    private final boolean organized;
    private final Map<String, String> properties;
    private final List<CStoreIndexHandle> indexHandles;

    @JsonCreator
    public CStoreOutputTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("transactionId") long transactionId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnHandles") List<CStoreColumnHandle> columnHandles,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("sortColumnHandles") List<CStoreColumnHandle> sortColumnHandles,
            @JsonProperty("sortOrders") List<SortOrder> sortOrders,
            @JsonProperty("temporalColumnHandle") Optional<CStoreColumnHandle> temporalColumnHandle,
            @JsonProperty("distributionId") OptionalLong distributionId,
            @JsonProperty("bucketCount") OptionalInt bucketCount,
            @JsonProperty("bucketColumnHandles") List<CStoreColumnHandle> bucketColumnHandles,
            @JsonProperty("organized") boolean organized,
            @JsonProperty("properties") Map<String, String> properties,
            @JsonProperty("indexHandles") List<CStoreIndexHandle> indexHandles)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.transactionId = transactionId;
        this.schemaName = checkSchemaName(schemaName);
        this.tableName = checkTableName(tableName);
        this.columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
        this.indexHandles = ImmutableList.copyOf(requireNonNull(indexHandles, "indexHandles is null"));
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        this.sortOrders = requireNonNull(sortOrders, "sortOrders is null");
        this.sortColumnHandles = requireNonNull(sortColumnHandles, "sortColumnHandles is null");
        this.temporalColumnHandle = requireNonNull(temporalColumnHandle, "temporalColumnHandle is null");
        this.distributionId = requireNonNull(distributionId, "distributionId is null");
        this.bucketCount = requireNonNull(bucketCount, "bucketCount is null");
        this.bucketColumnHandles = ImmutableList.copyOf(requireNonNull(bucketColumnHandles, "bucketColumnHandles is null"));
        this.organized = organized;
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public long getTransactionId()
    {
        return transactionId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public List<CStoreColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @JsonProperty
    public List<CStoreIndexHandle> getIndexHandles()
    {
        return indexHandles;
    }

    @JsonProperty
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @JsonProperty
    public List<CStoreColumnHandle> getSortColumnHandles()
    {
        return sortColumnHandles;
    }

    @JsonProperty
    public List<SortOrder> getSortOrders()
    {
        return sortOrders;
    }

    @JsonProperty
    public Optional<CStoreColumnHandle> getTemporalColumnHandle()
    {
        return temporalColumnHandle;
    }

    @JsonProperty
    public OptionalLong getDistributionId()
    {
        return distributionId;
    }

    @JsonProperty
    public OptionalInt getBucketCount()
    {
        return bucketCount;
    }

    @JsonProperty
    public List<CStoreColumnHandle> getBucketColumnHandles()
    {
        return bucketColumnHandles;
    }

    @JsonProperty
    public boolean isOrganized()
    {
        return organized;
    }

    @JsonProperty
    public Map<String, String> getProperties()
    {
        return properties;
    }

    @Override
    public String toString()
    {
        return "raptor:" + schemaName + "." + tableName;
    }
}