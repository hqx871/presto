package org.apache.cstore.dictionary;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.common.block.BlockEncodingSerde;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class DictionaryValueEncoding
        implements BlockEncoding
{
    public static final String NAME = "CSTORE_DICTIONARY_VALUE";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput input)
    {
        int offsetSize = input.readInt();
        int[] offsetInts = new int[offsetSize];
        for (int i = 0; i < offsetSize; i++) {
            offsetInts[i] = input.readInt();
        }
        int valueSize = input.readInt();
        byte[] valueBytes = new byte[valueSize];
        for (int i = 0; i < valueSize; i++) {
            valueBytes[i] = input.readByte();
        }
        return new DictionaryValueBlock(ByteBuffer.wrap(valueBytes), IntBuffer.wrap(offsetInts));
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        DictionaryValueBlock valueBlock = (DictionaryValueBlock) block;
        IntBuffer offsetBuffer = valueBlock.getOffsetBuffer();
        sliceOutput.writeInt(offsetBuffer.limit());
        for (int i = 0; i < offsetBuffer.limit(); i++) {
            sliceOutput.writeInt(offsetBuffer.get(i));
        }
        ByteBuffer valueBuffer = valueBlock.getValueBuffer();
        sliceOutput.writeInt(valueBuffer.limit());
        for (int i = 0; i < valueBuffer.limit(); i++) {
            sliceOutput.writeByte(valueBuffer.get(i));
        }
    }
}
