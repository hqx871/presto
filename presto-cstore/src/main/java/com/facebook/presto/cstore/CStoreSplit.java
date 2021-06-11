package com.facebook.presto.cstore;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CStoreSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final Set<UUID> shardUuids;
    private final Map<UUID, UUID> shardDeltaMap;
    private final boolean tableSupportsDeltaDelete;
    private final OptionalInt bucketNumber;
    private final List<HostAddress> addresses;
    private final TupleDomain<CStoreColumnHandle> effectivePredicate;
    private final OptionalLong transactionId;
    private final Optional<Map<String, Type>> columnTypes;
    private final RowExpression filter;

    @JsonCreator
    public CStoreSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("shardUuids") Set<UUID> shardUuids,
            @JsonProperty("shardDeltaMap") Map<UUID, UUID> shardDeltaMap,
            @JsonProperty("tableSupportsDeltaDelete") boolean tableSupportsDeltaDelete,
            @JsonProperty("bucketNumber") OptionalInt bucketNumber,
            @JsonProperty("effectivePredicate") TupleDomain<CStoreColumnHandle> effectivePredicate,
            @JsonProperty("transactionId") OptionalLong transactionId,
            @JsonProperty("columnTypes") Optional<Map<String, Type>> columnTypes,
            @Nullable @JsonProperty("filter") RowExpression filter)
    {
        this(
                connectorId,
                shardUuids,
                shardDeltaMap,
                tableSupportsDeltaDelete,
                bucketNumber,
                ImmutableList.of(),
                effectivePredicate,
                transactionId,
                columnTypes,
                filter);
    }

    public CStoreSplit(
            String connectorId,
            UUID shardUuid,
            Optional<UUID> deltaShardUuid,
            boolean tableSupportsDeltaDelete,
            List<HostAddress> addresses,
            TupleDomain<CStoreColumnHandle> effectivePredicate,
            OptionalLong transactionId,
            Optional<Map<String, Type>> columnTypes,
            RowExpression filter)
    {
        this(
                connectorId,
                ImmutableSet.of(shardUuid),
                deltaShardUuid.map(deltaUuid -> ImmutableMap.of(shardUuid, deltaUuid)).orElse(ImmutableMap.of()),
                tableSupportsDeltaDelete,
                OptionalInt.empty(),
                addresses,
                effectivePredicate,
                transactionId,
                columnTypes,
                filter);
    }

    public CStoreSplit(
            String connectorId,
            Set<UUID> shardUuids,
            Map<UUID, UUID> shardDeltaMap,
            boolean tableSupportsDeltaDelete,
            int bucketNumber,
            HostAddress address,
            TupleDomain<CStoreColumnHandle> effectivePredicate,
            OptionalLong transactionId,
            Optional<Map<String, Type>> columnTypes,
            RowExpression filter)
    {
        this(
                connectorId,
                shardUuids,
                shardDeltaMap,
                tableSupportsDeltaDelete,
                OptionalInt.of(bucketNumber),
                ImmutableList.of(address),
                effectivePredicate,
                transactionId,
                columnTypes,
                filter);
    }

    private CStoreSplit(
            String connectorId,
            Set<UUID> shardUuids,
            Map<UUID, UUID> shardDeltaMap,
            boolean tableSupportsDeltaDelete,
            OptionalInt bucketNumber,
            List<HostAddress> addresses,
            TupleDomain<CStoreColumnHandle> effectivePredicate,
            OptionalLong transactionId,
            Optional<Map<String, Type>> columnTypes,
            RowExpression filter)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.shardUuids = ImmutableSet.copyOf(requireNonNull(shardUuids, "shardUuid is null"));
        this.shardDeltaMap = requireNonNull(shardDeltaMap, "shardUuid is null");
        this.tableSupportsDeltaDelete = tableSupportsDeltaDelete;
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.columnTypes = requireNonNull(columnTypes, "columnTypes is null");
        this.filter = filter;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return HARD_AFFINITY;
    }

    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return addresses;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public Set<UUID> getShardUuids()
    {
        return shardUuids;
    }

    @JsonProperty
    public Map<UUID, UUID> getShardDeltaMap()
    {
        return shardDeltaMap;
    }

    @JsonProperty
    public boolean isTableSupportsDeltaDelete()
    {
        return tableSupportsDeltaDelete;
    }

    @JsonProperty
    public OptionalInt getBucketNumber()
    {
        return bucketNumber;
    }

    @JsonProperty
    public TupleDomain<CStoreColumnHandle> getEffectivePredicate()
    {
        return effectivePredicate;
    }

    @JsonProperty
    public OptionalLong getTransactionId()
    {
        return transactionId;
    }

    @JsonProperty
    public Optional<Map<String, Type>> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @JsonProperty
    public RowExpression getFilter()
    {
        return filter;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("shardUuids", shardUuids)
                .add("shardDeltaMap", shardDeltaMap.toString())
                .add("tableSupportsDeltaDelete", tableSupportsDeltaDelete)
                .add("bucketNumber", bucketNumber.isPresent() ? bucketNumber.getAsInt() : null)
                .add("hosts", addresses)
                .omitNullValues()
                .toString();
    }
}
