package github.cstore.column;

import github.cstore.bitmap.Bitmap;
import github.cstore.coder.BitmapCoder;

import java.nio.ByteBuffer;

public final class BitmapColumnReader
        extends CacheVector<Bitmap>
        implements CStoreColumnReader
{
    private final BinaryOffsetColumnReader<Bitmap> delegate;

    public BitmapColumnReader(BinaryOffsetColumnReader<Bitmap> buffer)
    {
        this(buffer, new Bitmap[buffer.getRowCount()]);
    }

    private BitmapColumnReader(BinaryOffsetColumnReader<Bitmap> buffer, Bitmap[] cache)
    {
        super(buffer, cache);
        this.delegate = buffer;
    }

    public static Supplier newBuilder(ByteBuffer data)
    {
        BinaryOffsetColumnReader<Bitmap> buffer = BinaryOffsetColumnReader.decode(BitmapCoder::new, data);
        return new Supplier(new Bitmap[buffer.count()], buffer);
    }

    @Override
    public void setup()
    {
    }

    @Override
    public int getRowCount()
    {
        return delegate.getRowCount();
    }

    @Override
    public VectorCursor createVectorCursor(int size)
    {
        return delegate.createVectorCursor(size);
    }

    @Override
    public void close()
    {
    }

    public static class Supplier
            implements CStoreColumnReader.Supplier
    {
        private final Bitmap[] data;

        private final BinaryOffsetColumnReader<Bitmap> buffer;

        public Supplier(Bitmap[] data, BinaryOffsetColumnReader<Bitmap> buffer)
        {
            this.data = data;
            this.buffer = buffer;
        }

        @Override
        public BitmapColumnReader get()
        {
            return new BitmapColumnReader(buffer.duplicate(), data);
        }
    }
}
