package github.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import github.cstore.dictionary.StringArrayCacheDictionary;
import github.cstore.dictionary.StringDictionary;
import github.cstore.dictionary.StringLruCacheDictionary;
import io.airlift.compress.Decompressor;

import java.nio.ByteBuffer;

public final class StringEncodedColumnReader
        implements CStoreColumnReader
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
        this.dictionaryValue = dict.toBlock();
        this.idReader.setup();
    }

    public int decode(String value)
    {
        return getDictionary().lookupId(value);
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

    public static Supplier newBuilder(int rowCount, Type type, Decompressor decompressor, ByteBuffer data, StringDictionary dictionary)
    {
        data.position(0);
        byte coderId = data.get();
        data = data.slice();
        switch (coderId) {
            case ColumnEncodingId.PLAIN_BYTE: {
                StringArrayCacheDictionary cacheDict = new StringArrayCacheDictionary(dictionary);
                ByteColumnReaderFactory plainReaderFactory = new ByteColumnReaderFactory();
                ColumnChunkZipReader.Supplier chunkZipReaderBuilder = ColumnChunkZipReader.newBuilder(rowCount, data, decompressor, type, false, plainReaderFactory);
                return new Supplier(rowCount, type, chunkZipReaderBuilder, cacheDict);
            }
            case ColumnEncodingId.PLAIN_SHORT: {
                StringLruCacheDictionary cacheDict = new StringLruCacheDictionary(dictionary);
                ShortColumnReaderFactory plainReaderFactory = new ShortColumnReaderFactory();
                ColumnChunkZipReader.Supplier chunkZipReaderBuilder = ColumnChunkZipReader.newBuilder(rowCount, data, decompressor, type, false, plainReaderFactory);
                return new Supplier(rowCount, type, chunkZipReaderBuilder, cacheDict);
            }
            case ColumnEncodingId.PLAIN_INT: {
                StringLruCacheDictionary cacheDict = new StringLruCacheDictionary(dictionary);
                IntColumnReaderFactory plainReaderFactory = new IntColumnReaderFactory();
                ColumnChunkZipReader.Supplier chunkZipReaderBuilder = ColumnChunkZipReader.newBuilder(rowCount, data, decompressor, type, false, plainReaderFactory);
                return new Supplier(rowCount, type, chunkZipReaderBuilder, cacheDict);
            }
            default:
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public VectorCursor createVectorCursor(int size)
    {
        return new StringEncodedCursor(new int[size], dictionaryValue);
    }

    @Override
    public int read(int[] positions, int offset, int size, VectorCursor dst, int dstOffset)
    {
        return idReader.read(positions, offset, size, dst, dstOffset);
    }

    @Override
    public int read(int offset, int size, VectorCursor dst, int dstOffset)
    {
        return idReader.read(offset, size, dst, dstOffset);
    }

    @Override
    public void close()
    {
        this.idReader.close();
    }

    public static class Supplier
            implements CStoreColumnReader.Supplier
    {
        private final Type type;
        private final StringDictionary dict;
        private final int rowCount;
        private final CStoreColumnReader.Supplier idReader;

        public Supplier(int rowCount, Type type, CStoreColumnReader.Supplier idReader, StringDictionary dict)
        {
            this.type = type;
            this.dict = dict;
            this.rowCount = rowCount;
            this.idReader = idReader;
        }

        @Override
        public StringEncodedColumnReader get()
        {
            return new StringEncodedColumnReader(rowCount, type, idReader.get(), dict);
        }
    }
}
