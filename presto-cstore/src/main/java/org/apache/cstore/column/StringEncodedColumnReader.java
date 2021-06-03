package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.type.Type;
import org.apache.cstore.dictionary.StringArrayCacheDictionary;
import org.apache.cstore.dictionary.StringDictionary;
import org.apache.cstore.dictionary.StringLruCacheDictionary;
import org.apache.cstore.dictionary.TrieBufferTree;

import java.nio.ByteBuffer;

public abstract class StringEncodedColumnReader
        implements DictionaryReader
{
    protected final Type type;
    private final IntVector data;
    private final StringDictionary dict;
    private Block dictionaryValue;
    private final int rowCount;

    protected StringEncodedColumnReader(Type type, IntVector data, StringDictionary dict)
    {
        this.type = type;
        this.data = data;
        this.dict = dict;
        this.rowCount = data.getRowCount();
    }

    @Override
    public void setup()
    {
        this.dictionaryValue = dict.getDictionaryValue();
    }

    public int decode(String value)
    {
        return getDictionary().encodeId(value);
    }

    public StringDictionary getDictionary()
    {
        return dict;
    }

    @Override
    public int getRowCount()
    {
        return rowCount;
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

    @Override
    public Block getDictionaryValue()
    {
        return getDictionary().getDictionaryValue();
    }

    @Override
    public VectorCursor createVectorCursor(int size)
    {
        return new Cursor(new int[size], dictionaryValue);
    }

    @Override
    public int read(int[] positions, int offset, int size, VectorCursor dst)
    {
        int start = offset;
        for (int i = 0; i < size; i++) {
            int position = positions[start];
            int id = data.readInt(position);
            dst.writeInt(i, id);
            start++;
        }
        return size;
    }

    @Override
    public int read(int offset, int size, VectorCursor dst)
    {
        int start = offset;
        for (int i = 0; i < size; i++) {
            int id = data.readInt(start);
            dst.writeInt(i, id);
            start++;
        }
        return size;
    }

    protected static final class Cursor
            implements VectorCursor
    {
        private final int[] values;
        private final Block dictionary;
        private final int sizeInBytes;

        private Cursor(int[] values, Block dictionary)
        {
            this.values = values;
            this.dictionary = dictionary;
            this.sizeInBytes = (int) (getCapacity() * Integer.BYTES + dictionary.getSizeInBytes());
        }

        @Override
        public void writeInt(int position, int value)
        {
            values[position] = value;
        }

        @Override
        public int getSizeInBytes()
        {
            return sizeInBytes;
        }

        @Override
        public int getCapacity()
        {
            return values.length;
        }

        @Override
        public Block toBlock(int size)
        {
            return new DictionaryBlock(size, dictionary, values);
        }
    }

    @Override
    public int read(int[] positions, int offset, int size, BlockBuilder dst)
    {
        int start = offset;
        int end = start + size;
        while (start < end) {
            int position = positions[start];
            int id = data.readInt(position);
            dst.writeInt(id).closeEntry();
            start++;
        }
        return size;
    }

    @Override
    public int read(int offset, int size, BlockBuilder dst)
    {
        int start = offset;
        int end = start + size;
        while (start < end) {
            int id = data.readInt(start);
            dst.writeInt(id).closeEntry();
            start++;
        }
        return size;
    }

    @Override
    public void close()
    {
    }

    public IntVector getDataVector()
    {
        return data;
    }
}
