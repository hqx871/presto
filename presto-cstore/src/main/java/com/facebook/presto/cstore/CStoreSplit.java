package com.facebook.presto.cstore;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;

import java.util.List;
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
    private final OptionalInt bucketNumber;
    private final List<HostAddress> addresses;
    private final TupleDomain<CStoreColumnHandle> effectivePredicate;
    private final OptionalLong transactionId;
    private final RowExpression filter;

    @JsonCreator
    public CStoreSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("shardUuids") Set<UUID> shardUuids,
            @JsonProperty("bucketNumber") OptionalInt bucketNumber,
            @JsonProperty("effectivePredicate") TupleDomain<CStoreColumnHandle> effectivePredicate,
            @JsonProperty("transactionId") OptionalLong transactionId,
            @Nullable @JsonProperty("filter") RowExpression filter)
    {
        this(
                connectorId,
                shardUuids,
                bucketNumber,
                ImmutableList.of(),
                effectivePredicate,
                transactionId,
                filter);
    }

    public CStoreSplit(
            String connectorId,
            UUID shardUuid,
            List<HostAddress> addresses,
            TupleDomain<CStoreColumnHandle> effectivePredicate,
            OptionalLong transactionId,
            RowExpression filter)
    {
        this(
                connectorId,
                ImmutableSet.of(shardUuid),
                OptionalInt.empty(),
                addresses,
                effectivePredicate,
                transactionId,
                filter);
    }

    public CStoreSplit(
            String connectorId,
            Set<UUID> shardUuids,
            int bucketNumber,
            HostAddress address,
            TupleDomain<CStoreColumnHandle> effectivePredicate,
            OptionalLong transactionId,
            RowExpression filter)
    {
        this(
                connectorId,
                shardUuids,
                OptionalInt.of(bucketNumber),
                ImmutableList.of(address),
                effectivePredicate,
                transactionId,
                filter);
    }

    private CStoreSplit(
            String connectorId,
            Set<UUID> shardUuids,
            OptionalInt bucketNumber,
            List<HostAddress> addresses,
            TupleDomain<CStoreColumnHandle> effectivePredicate,
            OptionalLong transactionId,
            RowExpression filter)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.shardUuids = ImmutableSet.copyOf(requireNonNull(shardUuids, "shardUuid is null"));
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
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
                .add("bucketNumber", bucketNumber.isPresent() ? bucketNumber.getAsInt() : null)
                .add("hosts", addresses)
                .omitNullValues()
                .toString();
    }
}
