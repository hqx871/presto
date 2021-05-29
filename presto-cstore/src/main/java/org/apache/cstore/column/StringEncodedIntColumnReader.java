package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;
import org.apache.cstore.dictionary.StringDictionary;

public class StringEncodedIntColumnReader
        extends StringEncodedColumnReader
{
    private final IntColumnReader data;
    private final StringDictionary dict;

    public StringEncodedIntColumnReader(IntColumnReader data, StringDictionary dict)
    {
        this.data = data;
        this.dict = dict;
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
