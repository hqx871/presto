package github.cstore.dictionary;

import com.facebook.presto.common.block.Block;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;

import java.util.LinkedHashMap;
import java.util.Map;

public class StringLruCacheDictionary
        extends StringDictionary
{
    private final StringDictionary delegate;
    private final LinkedHashMap<Integer, String> cache;
    private final int cacheSize;

    public StringLruCacheDictionary(StringDictionary delegate)
    {
        this.delegate = delegate;
        this.cacheSize = 16 << 10;
        this.cache = new LinkedHashMap<Integer, String>()
        {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, String> eldest)
            {
                return size() >= cacheSize;
            }
        };
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
        return true;
    }

    @Override
    public Block toBlock()
    {
        return delegate.toBlock();
    }
}
