package org.apache.cstore.column;

import com.facebook.presto.common.type.Type;
import org.apache.cstore.dictionary.StringArrayCacheDictionary;
import org.apache.cstore.dictionary.StringDictionary;

public final class StringEncodedByteColumnReader
        extends StringEncodedColumnReader
{
    private final ByteColumnReader data;
    private final StringDictionary dict;
    private final String[] value;

    public StringEncodedByteColumnReader(Type type, ByteColumnReader data, StringArrayCacheDictionary dict)
    {
        super(type, data, dict);
        this.data = data;
        this.dict = dict;
        this.value = dict.value();
    }
}
