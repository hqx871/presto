package github.cstore.dictionary;

import com.facebook.presto.common.block.Block;

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
        this.cache = new String[delegate.getMaxId()];
        this.maxEncodeId = delegate.getMaxId();
        this.count = delegate.count();

        warnUp();
    }

    //@Override
    private void warnUp()
    {
        for (int i = 1; i < maxEncodeId; i++) {
            cache[i] = delegate.lookupValue(i);
        }
    }

    @Override
    public int lookupId(String value)
    {
        return delegate.lookupId(value);
    }

    @Override
    public String lookupValue(int id)
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
    public int getMaxId()
    {
        return maxEncodeId;
    }

    public String[] value()
    {
        return cache;
    }

    @Override
    public boolean isSort()
    {
        return delegate.isSort();
    }

    @Override
    public Block toBlock()
    {
        return delegate.toBlock();
    }
}
