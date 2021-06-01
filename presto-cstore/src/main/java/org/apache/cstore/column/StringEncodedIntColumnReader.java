package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import org.apache.cstore.dictionary.StringDictionary;

public class StringEncodedIntColumnReader
        extends StringEncodedColumnReader
{
    private final IntColumnReader data;
    private final StringDictionary dict;

    public StringEncodedIntColumnReader(Type type, IntColumnReader data, StringDictionary dict)
    {
        super(type);
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
        int start = offset;
        int end = start + size;
        while (start < end) {
            int position = positions[start];
            int id = data.readInt(position);
            dst.writeInt(id);
            start++;
        }
        return size;
    }

    @Override
    public int read(int offset, int size, BlockBuilder dst)
    {
        int start = offset;
        int end = start + size;
        while (start < end) {
            int id = data.readInt(start);
            dst.writeInt(id);
            start++;
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
