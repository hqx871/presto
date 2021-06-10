package github.cstore.dictionary;

import com.facebook.presto.common.block.Block;
import github.cstore.sort.BufferComparator;

public class StringArrayCacheDictionary
        extends StringDictionary
{
    private final StringDictionary delegate;
    private final String[] cache;
    private final int maxEncodeId;
    private final int count;

    public StringArrayCacheDictionary(StringDictionary delegate)
    {
        this.delegate = delegate;
        this.cache = new String[delegate.maxEncodeId()];
        this.maxEncodeId = delegate.maxEncodeId();
        this.count = delegate.count();

        warnUp();
    }

    //@Override
    private void warnUp()
    {
        for (int i = 1; i < maxEncodeId; i++) {
            cache[i] = delegate.decodeValue(i);
        }
    }

    @Override
    public int encodeId(String value)
    {
        return delegate.encodeId(value);
    }

    @Override
    public String decodeValue(int id)
    {
        if (id == 0) {
            return null;
        }
        return cache[id];
    }

    @Override
    public int count()
    {
        return count;
    }

    @Override
    public int maxEncodeId()
    {
        return maxEncodeId;
    }

    public String[] value()
    {
        return cache;
    }

    @Override
    public BufferComparator encodeComparator()
    {
        return delegate.encodeComparator();
    }

    @Override
    public boolean isSort()
    {
        return true;
    }

    @Override
    public Block getDictionaryValue()
    {
        return delegate.getDictionaryValue();
    }
}
