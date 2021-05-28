package com.facebook.presto.cstore.block;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockUtil;
import com.facebook.presto.common.block.IntArrayBlock;
import com.facebook.presto.common.block.IntArrayBlockEncoding;
import io.airlift.slice.SliceOutput;
import org.apache.cstore.bitmap.Bitmap;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.nio.IntBuffer;
import java.util.Optional;
import java.util.function.BiConsumer;

public final class IntBufferBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(IntBufferBlock.class).instanceSize();

    private final IntBuffer value;
    @Nullable
    private final Bitmap valueIsNull;
    private final int sizeInBytes;
    private final int positionCount;
    private final int offsetBase;

    public IntBufferBlock(IntBuffer value, @Nullable Bitmap valueIsNull)
    {
        this(value, valueIsNull, value.capacity(), 0);
    }

    private IntBufferBlock(IntBuffer value, @Nullable Bitmap valueIsNull, int positionCount, int offsetBase)
    {
        this.value = value;
        this.valueIsNull = valueIsNull;
        this.positionCount = positionCount;
        this.offsetBase = offsetBase;
        this.sizeInBytes = positionCount * Integer.BYTES + (valueIsNull == null ? 0 : valueIsNull.sizeInByte());
    }

    @Override
    public int getInt(int position)
    {
        checkReadablePosition(position);
        return getIntUnchecked(position);
    }

    @Override
    public int getIntUnchecked(int internalPosition)
    {
        return value.get(offsetBase + internalPosition);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        blockBuilder.writeInt(value.get(offsetBase + position));
    }

    @Override
    public void writePositionTo(int position, SliceOutput output)
    {
        checkReadablePosition(position);
        position += offsetBase;
        if (isNull(position)) {
            output.writeByte(0);
        }
        else {
            output.writeByte(1);
            output.writeInt(value.get(position));
        }
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return new IntArrayBlock(
                1,
                isNull(position) ? Optional.of(new boolean[] {true}) : Optional.empty(),
                new int[] {value.get(position + offsetBase)});
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
        return Integer.BYTES + Byte.BYTES;
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
        consumer.accept(value, (long) (positionCount * Integer.BYTES));
        if (valueIsNull != null) {
            consumer.accept(valueIsNull, (long) valueIsNull.sizeInByte());
        }
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        //todo use IntBufferBlock?
        return IntArrayBlockEncoding.NAME;
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        BlockUtil.checkArrayRange(positions, offset, length);

        boolean[] newValueIsNull = null;
        if (valueIsNull != null) {
            newValueIsNull = new boolean[length];
        }
        int[] newValues = new int[length];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(position);
            if (valueIsNull != null) {
                newValueIsNull[i] = valueIsNull.get(position + offsetBase);
            }
            newValues[i] = value.get(position + offsetBase);
        }
        return new IntArrayBlock(length, Optional.ofNullable(newValueIsNull), newValues);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        BlockUtil.checkValidRegion(getPositionCount(), positionOffset, length);
        return new IntBufferBlock(value, valueIsNull, length, offsetBase + positionOffset);
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        BlockUtil.checkValidRegion(getPositionCount(), position, length);
        boolean[] newValueIsNull = valueIsNull == null ? null : valueIsNull.toBoolArray(position, length);
        int[] newValues = BlockUtil.compactArray(value, position, length);
        return new IntArrayBlock(length, Optional.ofNullable(newValueIsNull), newValues);
    }

    @Override
    public boolean isNull(int position)
    {
        return valueIsNull != null && valueIsNull.get(position);
    }

    @Override
    public Block appendNull()
    {
        throw new UnsupportedOperationException("int buffer is immutable");
    }

    @Override
    public boolean isNullUnchecked(int internalPosition)
    {
        assert mayHaveNull() : "no nulls present";
        assert BlockUtil.internalPositionInRange(internalPosition, getOffsetBase(), getPositionCount());
        return valueIsNull.get(internalPosition);
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
}
