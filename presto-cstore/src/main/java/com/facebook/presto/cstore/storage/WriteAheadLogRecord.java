package com.facebook.presto.cstore.storage;

import java.util.UUID;

public class WriteAheadLogRecord
{
    private final long transactionId;
    private final UUID shardUuid;
    private final ActionType actionType;
    private final byte[] argument;

    public WriteAheadLogRecord(long transactionId, UUID shardUuid, ActionType actionType, byte[] argument)
    {
        this.transactionId = transactionId;
        this.shardUuid = shardUuid;
        this.actionType = actionType;
        this.argument = argument;
    }

    public long getTransactionId()
    {
        return transactionId;
    }

    public UUID getShardUuid()
    {
        return shardUuid;
    }

    public ActionType getActionType()
    {
        return actionType;
    }

    public byte[] getArgument()
    {
        return argument;
    }

    public int getBytesSize()
    {
        return Long.SIZE * 3 + Integer.BYTES * 2 + argument.length;
    }
}
