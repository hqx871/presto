package github.cstore.dictionary;

import com.facebook.presto.common.block.Block;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;

public class StringLruCacheDictionary
        extends StringDictionary
{
    private final StringDictionary delegate;
    private final Int2ObjectArrayMap<String> cache;

    public StringLruCacheDictionary(StringDictionary delegate)
    {
        this.delegate = delegate;
        this.cache = new Int2ObjectArrayMap<>();
    }

    @Override
    public int encodeId(String value)
    {
        int id = delegate.encodeId(value);
        cache.put(id, value);
        return id;
    }

    @Override
    public String decodeValue(int id)
    {
        String decodeValue = cache.get(id);
        if (decodeValue == null && id != 0) {
            decodeValue = delegate.decodeValue(id);
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
    public int maxEncodeId()
    {
        return delegate.maxEncodeId();
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
