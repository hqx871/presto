package org.apache.cstore.column;

import org.apache.cstore.dictionary.StringArrayCacheDictionary;
import org.apache.cstore.dictionary.StringLruCacheDictionary;
import org.apache.cstore.dictionary.TrieBufferTree;

import java.nio.ByteBuffer;

public abstract class StringEncodedColumnReader
        implements CStoreColumnReader
{
    public static StringEncodedColumnReader decode(ByteBuffer data, ByteBuffer dict)
    {
        TrieBufferTree trieDict = TrieBufferTree.decode(dict);
        data.position(0);
        byte coderId = data.get();
        data = data.slice();
        switch (coderId) {
            case ColumnEncodingId.PLAIN_BYTE: {
                StringArrayCacheDictionary cacheDict = new StringArrayCacheDictionary(trieDict);
                return new StringEncodedByteColumnReader(new ByteColumnReader(data), cacheDict);
            }
            case ColumnEncodingId.PLAIN_SHORT: {
                StringLruCacheDictionary cacheDict = new StringLruCacheDictionary(trieDict);
                return new StringEncodedShortColumnReader(new ShortColumnReader(data.asShortBuffer()), cacheDict);
                //return new StringEncodedShortVector(ShortVectorReader.decode(data.asShortBuffer()), trieDict);
            }
            case ColumnEncodingId.PLAIN_INT: {
                StringLruCacheDictionary cacheDict = new StringLruCacheDictionary(trieDict);
                return new StringEncodedIntColumnReader(new IntColumnReader(data.asIntBuffer()), cacheDict);
            }
            default:
        }
        throw new UnsupportedOperationException();
    }
}
