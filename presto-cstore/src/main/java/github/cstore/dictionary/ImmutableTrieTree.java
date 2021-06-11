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

public class ImmutableTrieTree
        extends StringDictionary
{
    private final BinaryOffsetVector<String> noNullValues;
    private final ByteBuffer treeBuffer;
    private final byte nullId;
    private final Block dictionaryBlock;

    public ImmutableTrieTree(BinaryOffsetVector<String> noNullValues, ByteBuffer treeBuffer, byte nullId)
    {
        this.noNullValues = noNullValues;
        this.treeBuffer = treeBuffer;
        this.nullId = nullId;
        this.dictionaryBlock = buildDictionaryValue();
    }

    private int searchChildOffset(int offset, int from, int to, char[] value, int start)
    {
        int pos = binarySearch(offset, from, to, value, start);
        if (pos >= 0) {
            return treeBuffer.getInt(offset + pos * 6 + 2);
        }
        else {
            return -1;
        }
    }

    private int binarySearch(int offset, int from, int to, char[] value, int start)
    {
        if (from > to) {
            return -1 - from;
        }
        int middle = (from + to) / 2;
        int compared = value[start] - treeBuffer.getChar(offset + middle * 6);
        if (compared == 0) {
            return middle;
        }
        if (compared > 0) {
            return binarySearch(offset, middle + 1, to, value, start);
        }
        else {
            return binarySearch(offset, from, middle - 1, value, start);
        }
    }

    private int sameValueLength(int offset, char[] that, int start)
    {
        int charCount = treeBuffer.getInt(offset - 8);
        int childCount = treeBuffer.getInt(offset - 4);
        int valueOffset = offset - 12 - childCount * 6 - charCount * 2;
        int n = Math.min(charCount, that.length - start);
        for (int i = 0; i < n; i++) {
            char character = treeBuffer.getChar(valueOffset + i * 2);
            if (character != that[i + start]) {
                return i;
            }
        }
        return n;
    }

    @Override
    public int encodeId(String value)
    {
        if (value == null) {
            return nullId;
        }
        return idNoNull(value.toCharArray());
    }

    private int idNoNull(char[] value)
    {
        if (value.length == 0) {
            return treeBuffer.getInt(treeBuffer.limit() - 12);
        }
        return idNoEmpty(treeBuffer.limit(), value, 0);
    }

    private int idNoEmpty(int limit, char[] value, int start)
    {
        int childCount = treeBuffer.getInt(limit - 4);
        int childIndexOffset = limit - 12 - childCount * 6;
        int childOffset = searchChildOffset(childIndexOffset, 0, childCount - 1, value, start);
        if (childOffset >= 0) {
            int sameValueLength = sameValueLength(childOffset, value, start + 1);
            int sameNodeValueLength = treeBuffer.getInt(childOffset - 8);
            if (sameValueLength + start + 1 == value.length) {
                if (sameValueLength == sameNodeValueLength) {
                    return treeBuffer.getInt(childOffset - 12);
                }
                else {
                    return INVALID_ID;
                }
            }
            else {
                if (sameValueLength < sameNodeValueLength) {
                    return INVALID_ID;
                }
                else {
                    return idNoEmpty(childOffset, value, start + 1 + sameValueLength);
                }
            }
        }
        else {
            return INVALID_ID;
        }
    }

    @Override
    public String decodeValue(int id)
    {
        //Preconditions.checkArgument(id >= 0);
        if (id == 0) {
            //Preconditions.checkState(nullId != INVALID_ID);
            return null;
        }
        return noNullValues.readObject(id - getNonNullValueStartId());
    }

    @Override
    public int count()
    {
        //todo include null?
        return noNullValues.count() + (nullId == INVALID_ID ? 0 : 1);
    }

    @Override
    public int maxEncodeId()
    {
        return noNullValues.count() + 1;
    }

    public static ImmutableTrieTree decode(ByteBuffer tree, ByteBuffer sst)
    {
        byte nullId = sst.get(0);
        sst.position(Byte.BYTES);
        ByteBuffer sstData = sst.slice();
        return new ImmutableTrieTree(BinaryOffsetVector.decode(BufferCoder.UTF8, sstData), tree, nullId);
    }

    @Override
    public BufferComparator encodeComparator()
    {
        return new BufferComparator()
        {
            @Override
            public int compare(ByteBuffer a, int oa, ByteBuffer b, int ob)
            {
                return Integer.compare(a.getInt(oa), b.getInt(ob));
            }
        };
    }

    @Override
    public boolean isSort()
    {
        return true;
    }

    @Override
    public Block getDictionaryValue()
    {
        return dictionaryBlock;
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
