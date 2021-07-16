package com.facebook.presto.cstore.storage;

import java.util.UUID;

public interface WriteAheadLogAppender
{
    void append(long transaction, UUID shardUuid, ActionType command, byte[] args);

    void close();
}
