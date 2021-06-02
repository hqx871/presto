package org.apache.cstore.column;

import com.facebook.presto.common.type.Type;
import org.apache.cstore.dictionary.StringDictionary;

public final class StringEncodedShortColumnReader
        extends StringEncodedColumnReader
{
    private final ShortColumnReader data;
    private final StringDictionary dict;

    public StringEncodedShortColumnReader(Type type, ShortColumnReader data, StringDictionary dict)
    {
        super(type, data, dict);
        this.data = data;
        this.dict = dict;
    }
}
