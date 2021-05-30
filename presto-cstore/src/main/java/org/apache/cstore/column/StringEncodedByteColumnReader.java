package org.apache.cstore.column;

import com.facebook.presto.common.block.AbstractArrayBlock;
import com.facebook.presto.common.block.AbstractVariableWidthBlock;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import org.apache.cstore.dictionary.StringArrayCacheDictionary;
import org.apache.cstore.dictionary.StringDictionary;

import javax.annotation.Nullable;

import java.util.function.BiConsumer;

public class StringEncodedByteColumnReader
        extends StringEncodedColumnReader
{
    private final ByteColumnReader data;
    private final StringDictionary dict;
    private final String[] value;

    public StringEncodedByteColumnReader(Type type, ByteColumnReader data, StringArrayCacheDictionary dict)
    {
        super(type);
        this.data = data;
        this.dict = dict;
        this.value = dict.value();
    }

    @Override
    public void setup()
    {
    }

    @Override
    public Block getDictionaryValue()
    {
        return new AbstractArrayBlock() {
            @Override
            protected Block getRawElementBlock()
            {
                return null;
            }

            @Override
            protected int[] getOffsets()
            {
                return new int[0];
            }

            @Override
            public int getOffsetBase()
            {
                return 0;
            }

            @Nullable
            @Override
            protected boolean[] getValueIsNull()
            {
                return new boolean[0];
            }

            @Override
            public int getPositionCount()
            {
                return 0;
            }

            @Override
            public long getSizeInBytes()
            {
                return 0;
            }

            @Override
            public long getRetainedSizeInBytes()
            {
                return 0;
            }

            @Override
            public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
            {

            }
        };
    }

    @Override
    public int read(int[] positions, int offset, int size, BlockBuilder dst)
    {
        for (int i = 0; i < size; i++) {
            int position = positions[i + offset];
            int id = data.readInt(position);
            String value = dict.decodeValue(id);
            appendTo(value, dst);
        }
        return size;
    }

    @Override
    public int read(int offset, int size, BlockBuilder dst)
    {
        for (int i = 0; i < size; i++) {
            int position = offset + i;
            int id = data.readInt(position);
            String value = dict.decodeValue(id);
            appendTo(value, dst);
        }
        return size;
    }

    @Override
    public void close()
    {
    }

    @Override
    public StringDictionary getDictionary()
    {
        return dict;
    }
}
