package github.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import github.cstore.dictionary.StringArrayCacheDictionary;
import github.cstore.dictionary.StringDictionary;
import github.cstore.dictionary.StringLruCacheDictionary;
import io.airlift.compress.Decompressor;

import java.nio.ByteBuffer;

public final class StringEncodedColumnReader
        implements DictionaryReader
{
    protected final Type type;
    private final StringDictionary dict;
    private Block dictionaryValue;
    private final int rowCount;
    protected final CStoreColumnReader idReader;

    protected StringEncodedColumnReader(int rowCount, Type type, CStoreColumnReader idReader, StringDictionary dict)
    {
        this.type = type;
        this.dict = dict;
        this.rowCount = rowCount;
        this.idReader = idReader;
    }

    @Override
    public void setup()
    {
        this.dictionaryValue = dict.getDictionaryValue();
        this.idReader.setup();
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

    public static Builder newBuilder(int rowCount, int pageRowCount, Type type, Decompressor decompressor, ByteBuffer data, StringDictionary dictionary)
    {
        data.position(0);
        byte coderId = data.get();
        data = data.slice();
        switch (coderId) {
            case ColumnEncodingId.PLAIN_BYTE: {
                StringArrayCacheDictionary cacheDict = new StringArrayCacheDictionary(dictionary);
                return new Builder(rowCount, type, ByteColumnZipReader.newBuilder(rowCount, pageRowCount, data, decompressor, TinyintType.TINYINT, false), cacheDict);
            }
            case ColumnEncodingId.PLAIN_SHORT: {
                StringLruCacheDictionary cacheDict = new StringLruCacheDictionary(dictionary);
                return new Builder(rowCount, type, ShortColumnZipReader.newBuilder(rowCount, pageRowCount, data, decompressor, SmallintType.SMALLINT, false), cacheDict);
            }
            case ColumnEncodingId.PLAIN_INT: {
                StringLruCacheDictionary cacheDict = new StringLruCacheDictionary(dictionary);
                return new Builder(rowCount, type, IntColumnZipReader.newBuilder(rowCount, pageRowCount, data, decompressor, IntegerType.INTEGER, false), cacheDict);
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
        return new StringEncodedCursor(new int[size], dictionaryValue);
    }

    @Override
    public int read(int[] positions, int offset, int size, VectorCursor dst)
    {
        return idReader.read(positions, offset, size, dst);
    }

    @Override
    public int read(int offset, int size, VectorCursor dst)
    {
        return idReader.read(offset, size, dst);
    }

    @Override
    public int read(int[] positions, int offset, int size, BlockBuilder dst)
    {
        return idReader.read(positions, offset, size, dst);
    }

    @Override
    public int read(int offset, int size, BlockBuilder dst)
    {
        return idReader.read(offset, size, dst);
    }

    @Override
    public void close()
    {
        this.idReader.close();
    }

    public static class Builder
            implements CStoreColumnReader.Builder
    {
        private final Type type;
        private final StringDictionary dict;
        private final int rowCount;
        private final CStoreColumnReader.Builder idReader;

        public Builder(int rowCount, Type type, CStoreColumnReader.Builder idReader, StringDictionary dict)
        {
            this.type = type;
            this.dict = dict;
            this.rowCount = rowCount;
            this.idReader = idReader;
        }

        @Override
        public StringEncodedColumnReader build()
        {
            return new StringEncodedColumnReader(rowCount, type, idReader.build(), dict);
        }
    }
}
