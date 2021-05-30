package com.facebook.presto.cstore.block;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockUtil;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.apache.cstore.dictionary.DictionaryBlockValueEncoding;
import org.openjdk.jol.info.ClassLayout;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.function.BiConsumer;

public final class DictionaryValueBinaryBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DictionaryValueBinaryBlock.class).instanceSize();

    private final IntBuffer offsetBuffer;
    private final ByteBuffer valueBuffer;
    private final int sizeInBytes;
    private final int positionCount;
    private final int offsetBase;

    public DictionaryValueBinaryBlock(ByteBuffer valueBuffer, IntBuffer offsetBuffer)
    {
        this(valueBuffer, offsetBuffer, valueBuffer.limit() + 1, 0);
    }

    private DictionaryValueBinaryBlock(ByteBuffer valueBuffer, IntBuffer offsetBuffer, int positionCount, int offsetBase)
    {
        this.valueBuffer = valueBuffer;
        this.positionCount = positionCount;
        this.offsetBase = offsetBase;
        this.sizeInBytes = offsetBuffer.limit() * Integer.BYTES + valueBuffer.limit();
        this.offsetBuffer = offsetBuffer;
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writePositionTo(int position, SliceOutput output)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return new DictionaryValueBinaryBlock(
                valueBuffer,
                offsetBuffer,
                1,
                position + offsetBase);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        checkReadablePosition(position);
        return getSliceLengthUnchecked(position) + Integer.BYTES;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        return (Integer.BYTES + Byte.BYTES) * (long) BlockUtil.countUsedPositions(positions);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return 0;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return isNull(position) ? 0 : Integer.BYTES;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(valueBuffer, (long) (positionCount * Integer.BYTES));
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        return DictionaryBlockValueEncoding.NAME;
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        BlockUtil.checkArrayRange(positions, offset, length);
        throw new UnsupportedOperationException();
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        BlockUtil.checkValidRegion(getPositionCount(), positionOffset, length);
        return new DictionaryValueBinaryBlock(valueBuffer, offsetBuffer, length, offsetBase + positionOffset);
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        BlockUtil.checkValidRegion(getPositionCount(), position, length);
        return new DictionaryValueBinaryBlock(valueBuffer, offsetBuffer, length, position + offsetBase);
    }

    @Override
    public boolean isNull(int position)
    {
        return isNullUnchecked(position);
    }

    @Override
    public Block appendNull()
    {
        throw new UnsupportedOperationException("int buffer is immutable");
    }

    @Override
    public boolean isNullUnchecked(int internalPosition)
    {
        return internalPosition == 0;
    }

    @Override
    public int getOffsetBase()
    {
        return offsetBase;
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }

    @Override
    public int getSliceLength(int position)
    {
        checkReadablePosition(position);
        return getSliceLengthUnchecked(position);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        checkReadablePosition(position);
        return getSliceUnchecked(position, offset, length);
    }

    @Override
    public int getSliceLengthUnchecked(int position)
    {
        if (position == 0) {
            return 0;
        }
        return offsetBuffer.get(position + offsetBase) - offsetBuffer.get(position + offsetBase - 1);
    }

    @Override
    public Slice getSliceUnchecked(int position, int offset, int length)
    {
        if (position == 0) {
            return Slices.wrappedBuffer();
        }
        int start = this.offsetBuffer.get(offsetBase + position - 1);
        valueBuffer.position(start);
        ByteBuffer slice = valueBuffer.slice();
        slice.limit(this.offsetBuffer.get(offsetBase + position) - start);
        return Slices.wrappedBuffer(slice).slice(offset, length);
    }

    public IntBuffer getOffsetBuffer()
    {
        return offsetBuffer;
    }

    public ByteBuffer getValueBuffer()
    {
        return valueBuffer;
    }
}
