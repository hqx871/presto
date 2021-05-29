package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import io.airlift.slice.Slices;
import org.apache.cstore.dictionary.StringArrayCacheDictionary;
import org.apache.cstore.dictionary.StringLruCacheDictionary;
import org.apache.cstore.dictionary.TrieBufferTree;

import java.nio.ByteBuffer;

public abstract class StringEncodedColumnReader
        implements CStoreColumnReader
{
    protected final Type type;

    protected StringEncodedColumnReader(Type type)
    {
        this.type = type;
    }

    protected void appendTo(String value, BlockBuilder dst)
    {
        if (value == null) {
            dst.appendNull();
        }
        else {
            type.writeSlice(dst, Slices.utf8Slice(value));
        }
    }

    public static StringEncodedColumnReader decode(Type type, ByteBuffer data, ByteBuffer dict)
    {
        TrieBufferTree trieDict = TrieBufferTree.decode(dict);
        data.position(0);
        byte coderId = data.get();
        data = data.slice();
        switch (coderId) {
            case ColumnEncodingId.PLAIN_BYTE: {
                StringArrayCacheDictionary cacheDict = new StringArrayCacheDictionary(trieDict);
                return new StringEncodedByteColumnReader(type, new ByteColumnReader(data), cacheDict);
            }
            case ColumnEncodingId.PLAIN_SHORT: {
                StringLruCacheDictionary cacheDict = new StringLruCacheDictionary(trieDict);
                return new StringEncodedShortColumnReader(type, new ShortColumnReader(data.asShortBuffer()), cacheDict);
                //return new StringEncodedShortVector(ShortVectorReader.decode(data.asShortBuffer()), trieDict);
            }
            case ColumnEncodingId.PLAIN_INT: {
                StringLruCacheDictionary cacheDict = new StringLruCacheDictionary(trieDict);
                return new StringEncodedIntColumnReader(type, new IntColumnReader(data.asIntBuffer()), cacheDict);
            }
            default:
        }
        throw new UnsupportedOperationException();
    }
}
