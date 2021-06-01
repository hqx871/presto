package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import org.apache.cstore.dictionary.StringArrayCacheDictionary;
import org.apache.cstore.dictionary.StringDictionary;

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
