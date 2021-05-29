package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;
import org.apache.cstore.dictionary.StringArrayCacheDictionary;
import org.apache.cstore.dictionary.StringDictionary;

public class StringEncodedByteColumnReader
        extends StringEncodedColumnReader
{
    private final ByteColumnReader data;
    private final StringDictionary dict;
    private final String[] value;

    public StringEncodedByteColumnReader(ByteColumnReader data, StringArrayCacheDictionary dict)
    {
        this.data = data;
        this.dict = dict;
        this.value = dict.value();
    }

    @Override
    public void setup()
    {
    }

    @Override
    public int read(int[] positions, int offset, int size, BlockBuilder dst)
    {
        for (int i = 0; i < size; i++) {
            int position = positions[i + offset];
            dst.writeInt(data.readInt(position));
        }
        return size;
    }

    @Override
    public int read(int offset, int size, BlockBuilder dst)
    {
        for (int i = 0; i < size; i++) {
            int position = offset + i;
            dst.writeInt(data.readInt(position));
        }
        return size;
    }

    @Override
    public void close()
    {
    }
}
