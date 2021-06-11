package github.cstore.dictionary;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.VariableWidthBlock;
import github.cstore.coder.BufferCoder;
import github.cstore.column.BinaryOffsetVector;
import github.cstore.sort.BufferComparator;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;
import java.util.Optional;

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
        int position = -1;
        int minIndex = 0;
        int maxIndex = noNullValues.count() - 1;
        while (minIndex <= maxIndex) {
            int currIndex = (minIndex + maxIndex) >>> 1;

            String currValue = noNullValues.readObject(currIndex);
            int comparison = currValue.compareTo(value);
            if (comparison == 0) {
                position = currIndex;
                break;
            }

            if (comparison < 0) {
                minIndex = currIndex + 1;
            }
            else {
                maxIndex = currIndex - 1;
            }
        }
        if (position < 0) {
            return INVALID_ID;
        }
        return position + 1;
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

    @Override
    public Block getDictionaryValue()
    {
        return buildDictionaryValue();
    }

    private Block buildDictionaryValue()
    {
        int[] offsetVector = new int[noNullValues.getOffsetBuffer().limit() + 1];
        for (int i = 0; i < noNullValues.getOffsetBuffer().limit(); i++) {
            offsetVector[i + 1] = noNullValues.getOffsetBuffer().get(i);
        }
        ByteBuffer valueBuffer = noNullValues.getValueBuffer().asReadOnlyBuffer();
        valueBuffer.rewind();
        Slice valueSlice = Slices.wrappedBuffer(valueBuffer);
        int count = noNullValues.count() + 1;
        boolean[] isNullVector = new boolean[count];
        isNullVector[0] = true;
        return new VariableWidthBlock(count, valueSlice, offsetVector, Optional.of(isNullVector));
        //return new DictionaryValueBlock(noNullValues.getValueBuffer(), noNullValues.getOffsetBuffer());
    }
}
