package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;

public interface DictionaryReader
        extends CStoreColumnReader
{
    Block getDictionaryValue();
}
