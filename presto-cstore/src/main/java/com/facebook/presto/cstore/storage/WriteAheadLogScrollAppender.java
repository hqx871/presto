package com.facebook.presto.cstore.storage;

import java.util.UUID;

public class WriteAheadLogScrollAppender
        implements WriteAheadLogAppender
{
    private final long tableId;
    private final WriteAheadLogManager walManager;
    private WriteAheadLogFileAppender delegate;
    private final long transactionId;

    public WriteAheadLogScrollAppender(long tableId, WriteAheadLogManager walManager, long transactionId)
    {
        this.tableId = tableId;
        this.walManager = walManager;
        this.transactionId = transactionId;
    }

    @Override
    public void append(long transaction, UUID shardUuid, ActionType command, byte[] args)
    {
        createFileAppenderIfNecessary();
        delegate.append(transaction, shardUuid, command, args);
    }

    private void createFileAppenderIfNecessary()
    {
        if (delegate != null && delegate.isFull()) {
            walManager.commit(delegate.getWalUuid());
            delegate.close();
            delegate = null;
        }
        if (delegate == null) {
            delegate = walManager.createFileAppender(transactionId, UUID.randomUUID());
        }
    }

    @Override
    public void close()
    {
        if (delegate != null) {
            delegate.close();
        }
        delegate = null;
    }
}
