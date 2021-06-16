package github.cstore.dictionary;

import com.facebook.presto.common.block.Block;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;

public class StringMapCacheDictionary
        extends StringDictionary
{
    private final StringDictionary delegate;
    private final Int2ObjectArrayMap<String> cache;

    public StringMapCacheDictionary(StringDictionary delegate)
    {
        this.delegate = delegate;
        this.cache = new Int2ObjectArrayMap<>();
    }

    @Override
    public int lookupId(String value)
    {
        int id = delegate.lookupId(value);
        cache.put(id, value);
        return id;
    }

    @Override
    public String lookupValue(int id)
    {
        String decodeValue = cache.get(id);
        if (decodeValue == null && id != 0) {
            decodeValue = delegate.lookupValue(id);
            cache.put(id, decodeValue);
        }
        return decodeValue;
    }

    @Override
    public int count()
    {
        return delegate.count();
    }

    @Override
    public int getMaxId()
    {
        return delegate.getMaxId();
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
