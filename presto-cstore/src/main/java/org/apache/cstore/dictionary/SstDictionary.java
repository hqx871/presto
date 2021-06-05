package org.apache.cstore.dictionary;

import org.apache.cstore.BufferComparator;
import org.apache.cstore.coder.BufferCoder;
import org.apache.cstore.column.BinaryOffsetVector;

import java.nio.ByteBuffer;

public class SstDictionary
        extends StringDictionary
{
    private final BinaryOffsetVector<String> noNullValues;
    private final byte nullId;

    public SstDictionary(BinaryOffsetVector<String> noNullValues, byte nullId)
    {
        this.noNullValues = noNullValues;
        this.nullId = nullId;
    }

    @Override
    public BufferComparator encodeComparator()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSort()
    {
        return true;
    }

    @Override
    public int encodeId(String value)
    {
        if (value == null) {
            return nullId;
        }
        int position = binarySearch(value, 0, noNullValues.count() - 1);
        if (position < 0) {
            return INVALID_ID;
        }
        return position + 1;
    }

    private int binarySearch(String value, int from, int to)
    {
        if (from > to) {
            return -1;
        }
        int mid = (from + to) / 2;
        String midValue = noNullValues.readObject(mid);
        int diff = midValue.compareTo(value);
        if (diff == 0) {
            return mid;
        }
        else if (diff < 0) {
            return binarySearch(value, from, mid - 1);
        }
        else {
            return binarySearch(value, mid + 1, to);
        }
    }

    @Override
    public String decodeValue(int id)
    {
        if (id == nullId) {
            return null;
        }
        else {
            return noNullValues.readObject(id - getNonNullValueStartId());
        }
    }

    @Override
    public int count()
    {
        return noNullValues.count() + (nullId == INVALID_ID ? 0 : 1);
    }

    @Override
    public int maxEncodeId()
    {
        return noNullValues.count() + 1;
    }

    public static SstDictionary decode(ByteBuffer buffer)
    {
        byte nullId = buffer.get(0);
        buffer.position(Byte.BYTES);
        ByteBuffer sstData = buffer.slice();

        return new SstDictionary(BinaryOffsetVector.decode(BufferCoder.UTF8, sstData), nullId);
    }
}
