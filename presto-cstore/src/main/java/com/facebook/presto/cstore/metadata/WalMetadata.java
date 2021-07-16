package com.facebook.presto.cstore.metadata;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import static com.facebook.presto.cstore.util.UuidUtil.uuidFromBytes;

public class WalMetadata
{
    private final long walId;
    private final String nodeIdentifier;
    private final UUID walUuid;
    private final long baseOffset;
    private final long baseTransaction;

    public WalMetadata(long walId, String nodeIdentifier, UUID walUuid,
            long baseOffset, long baseTransaction)
    {
        this.walId = walId;
        this.nodeIdentifier = nodeIdentifier;
        this.walUuid = walUuid;
        this.baseOffset = baseOffset;
        this.baseTransaction = baseTransaction;
    }

    public static class Mapper
            implements ResultSetMapper<WalMetadata>
    {
        @Override
        public WalMetadata map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new WalMetadata(
                    r.getLong("wal_id"),
                    r.getString("node_identifier"),
                    uuidFromBytes(r.getBytes("wal_uuid")),
                    r.getLong("base_offset"),
                    r.getLong("transaction_start"));
        }
    }

    public UUID getWalUuid()
    {
        return walUuid;
    }

    public long getWalId()
    {
        return walId;
    }

    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    public long getBaseOffset()
    {
        return baseOffset;
    }

    public long getBaseTransaction()
    {
        return baseTransaction;
    }
}
