package com.facebook.presto.cstore.storage;

import io.airlift.slice.SliceInput;

import java.net.URI;
import java.util.Iterator;
import java.util.UUID;

public class WriteAheadLogReader
        implements Iterator<WriteAheadLogRecord>
{
    private final URI uri;
    private final SliceInput sliceInput;

    public WriteAheadLogReader(URI uri, SliceInput sliceInput)
    {
        this.uri = uri;
        this.sliceInput = sliceInput;
    }

    @Override
    public boolean hasNext()
    {
        return sliceInput.isReadable();
    }

    @Override
    public WriteAheadLogRecord next()
    {
        long transaction = sliceInput.readLong();
        long uuidMostBits = sliceInput.readLong();
        long uuidLeastBits = sliceInput.readLong();
        int code = sliceInput.readInt();
        int argLength = sliceInput.readInt();
        byte[] arg = new byte[argLength];
        sliceInput.read(arg);
        return new WriteAheadLogRecord(transaction, new UUID(uuidMostBits, uuidLeastBits), ActionType.valueOfCode(code), arg);
    }

    public URI getUri()
    {
        return uri;
    }
}
