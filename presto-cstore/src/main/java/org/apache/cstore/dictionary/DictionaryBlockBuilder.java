package org.apache.cstore.dictionary;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.block.DictionaryBlockEncoding;
import com.facebook.presto.common.block.DictionaryId;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.facebook.presto.common.block.BlockUtil.checkArrayRange;
import static com.facebook.presto.common.block.BlockUtil.checkValidPositions;
import static com.facebook.presto.common.block.BlockUtil.checkValidRegion;
import static com.facebook.presto.common.block.BlockUtil.countUsedPositions;
import static com.facebook.presto.common.block.BlockUtil.internalPositionInRange;

public class DictionaryBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DictionaryBlockBuilder.class).instanceSize() + ClassLayout.parseClass(DictionaryId.class).instanceSize();

    private BlockBuilderStatus status;
    private final Block dictionary;
    private final int[] blockBuilder;
    private final int offsetBase;
    private int positionCount;

    public DictionaryBlockBuilder(Block dictionary, int[] blockBuilder, BlockBuilderStatus status)
    {
        this(dictionary, blockBuilder, 0, status);
    }

    public DictionaryBlockBuilder(Block dictionary, int[] blockBuilder, int offsetBase, BlockBuilderStatus status)
    {
        this.dictionary = dictionary;
        this.blockBuilder = blockBuilder;
        this.offsetBase = offsetBase;
        this.status = status;
    }

    @Override
    public BlockBuilder closeEntry()
    {
        //todo what is this?
        return this;
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        blockBuilder.writeInt(getId(position));
    }

    @Override
    public void writePositionTo(int position, SliceOutput output)
    {
        output.writeInt(getId(position));
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        int id = getId(position);
        return dictionary.getSingleValueBlock(id);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        boolean[] used = new boolean[dictionary.getPositionCount()];
        for (int i = 0; i < positionCount; i++) {
            int position = getId(i);
            if (!used[position]) {
                used[position] = true;
            }
        }
        return dictionary.getPositionsSizeInBytes(used) + (Integer.BYTES * (long) positionCount);
    }

    @Override
    public long getRegionSizeInBytes(int positionOffset, int length)
    {
        if (positionOffset == 0 && length == getPositionCount()) {
            // Calculation of getRegionSizeInBytes is expensive in this class.
            // On the other hand, getSizeInBytes result is cached.
            return getSizeInBytes();
        }

        boolean[] used = new boolean[dictionary.getPositionCount()];
        for (int i = positionOffset; i < positionOffset + length; i++) {
            used[getId(i)] = true;
        }
        return dictionary.getPositionsSizeInBytes(used) + Integer.BYTES * (long) length;
    }

    public int getId(int position)
    {
        return blockBuilder[offsetBase + position];
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        checkValidPositions(positions, getPositionCount());

        boolean[] used = new boolean[dictionary.getPositionCount()];
        for (int i = 0; i < positions.length; i++) {
            if (positions[i]) {
                used[getId(i)] = true;
            }
        }
        return dictionary.getPositionsSizeInBytes(used) + (Integer.BYTES * (long) countUsedPositions(positions));
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return Integer.BYTES * (blockBuilder.length - getPositionCount());
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return dictionary.getEstimatedDataSizeForStats(getId(position)) + Integer.BYTES * getPositionCount();
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(dictionary, dictionary.getRetainedSizeInBytes());
        consumer.accept(blockBuilder, (long) (Integer.BYTES * (blockBuilder.length - getPositionCount())));
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        //todo rewrite encoding
        return DictionaryBlockEncoding.NAME;
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        IntArrayList positionsToCopy = new IntArrayList();
        Map<Integer, Integer> oldIndexToNewIndex = new HashMap<>();

        int[] newIds = new int[length];

        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            int oldIndex = getId(position);
            if (!oldIndexToNewIndex.containsKey(oldIndex)) {
                oldIndexToNewIndex.put(oldIndex, positionsToCopy.size());
                positionsToCopy.add(oldIndex);
            }
            newIds[i] = oldIndexToNewIndex.get(oldIndex);
        }
        return new DictionaryBlock(dictionary.copyPositions(positionsToCopy.toArray(new int[0]), 0, positionsToCopy.size()), newIds);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);
        return new DictionaryBlockBuilder(dictionary, blockBuilder, offsetBase + positionOffset, status);
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        checkValidRegion(getPositionCount(), position, length);
        int[] newIds = new int[length];
        for (int i = 0; i < length; i++) {
            newIds[i] = getId(i + position + offsetBase);
        }
        DictionaryBlock dictionaryBlock = new DictionaryBlock(dictionary, newIds);
        return dictionaryBlock.compact();
    }

    @Override
    public boolean isNull(int position)
    {
        return dictionary.isNull(getId(position));
    }

    @Override
    public BlockBuilder appendNull()
    {
        //todo null id?
        blockBuilder[positionCount++] = 0;
        return this;
    }

    @Override
    public BlockBuilder readPositionFrom(SliceInput input)
    {
        boolean isNull = input.readByte() == 0;
        if (isNull) {
            appendNull();
        }
        else {
            blockBuilder[positionCount++] = input.readInt();
            closeEntry();
        }
        return this;
    }

    @Override
    public Block build()
    {
        return new DictionaryBlock(positionCount, dictionary, blockBuilder);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        return newBlockBuilderLike(blockBuilderStatus, positionCount);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return new DictionaryBlockBuilder(dictionary, new int[expectedEntries], status);
    }

    @Override
    public boolean isNullUnchecked(int internalPosition)
    {
        assert mayHaveNull() : "no nulls present";
        assert internalPositionInRange(internalPosition, offsetBase, getPositionCount());
        return dictionary.isNull(getId(internalPosition));
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
    public BlockBuilder writeInt(int value)
    {
        blockBuilder[positionCount++] = value;
        return this;
    }
}
