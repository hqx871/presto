package org.apache.cstore.column;

import com.facebook.presto.common.type.Type;
import org.apache.cstore.dictionary.StringDictionary;

public final class StringEncodedIntColumnReader
        extends StringEncodedColumnReader
{
    private final IntColumnReader data;
    private final StringDictionary dict;

    public StringEncodedIntColumnReader(Type type, IntColumnReader data, StringDictionary dict)
    {
        super(type, data, dict);
        this.data = data;
        this.dict = dict;
    }
}
