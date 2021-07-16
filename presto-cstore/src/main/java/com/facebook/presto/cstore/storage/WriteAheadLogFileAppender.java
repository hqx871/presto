package com.facebook.presto.cstore.storage;

import io.airlift.slice.SliceOutput;

import java.io.IOException;
import java.util.UUID;

public class WriteAheadLogFileAppender
{
    private final UUID walUuid;
    private final int fileMaxSize;
    private final SliceOutput output;
    private int outSize;

    public WriteAheadLogFileAppender(UUID walUuid, int fileMaxSize, SliceOutput output)
    {
        this.walUuid = walUuid;
        this.fileMaxSize = fileMaxSize;
        this.output = output;
    }

    public void append(long transaction, UUID shardUuid, ActionType command, byte[] args)
    {
        output.writeLong(transaction);
        output.writeLong(shardUuid.getMostSignificantBits());
        output.writeLong(shardUuid.getLeastSignificantBits());
        output.writeInt(command.getCode());
        output.writeInt(args.length);
        output.writeBytes(args);
        try {
            output.flush();
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
        outSize += 3 * Long.BYTES + 2 * Integer.BYTES + args.length;
    }

    public boolean isFull()
    {
        return outSize >= fileMaxSize;
    }

    public void close()
    {
        try {
            output.close();
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public UUID getWalUuid()
    {
        return walUuid;
    }
}
