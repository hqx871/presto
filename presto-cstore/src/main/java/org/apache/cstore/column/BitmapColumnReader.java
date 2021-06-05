package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;
import org.apache.cstore.bitmap.Bitmap;
import org.apache.cstore.coder.BufferCoder;

import java.nio.ByteBuffer;

public final class BitmapColumnReader
        extends CacheVector<Bitmap>
        implements CStoreColumnReader
{
    public static final BufferCoder<Bitmap> coder = new BufferCoder<Bitmap>()
    {
        @Override
        public Bitmap decode(ByteBuffer data)
        {
            return Bitmap.create(data);
        }

        @Override
        public ByteBuffer encode(Bitmap object)
        {
            return object.encode();
        }
    };

    public BitmapColumnReader(BinaryOffsetVector<Bitmap> buffer)
    {
        this(buffer, new Bitmap[buffer.count()]);
    }

    private BitmapColumnReader(BinaryOffsetVector<Bitmap> buffer, Bitmap[] cache)
    {
        super(buffer, cache, coder);
    }

    public static BitmapColumnReader decode(ByteBuffer data)
    {
        BinaryOffsetVector<Bitmap> buffer = BinaryOffsetVector.decode(BitmapColumnReader.coder, data);
        return new BitmapColumnReader(buffer);
    }

    public static Builder newBuilder(ByteBuffer data)
    {
        BinaryOffsetVector<Bitmap> buffer = BinaryOffsetVector.decode(BitmapColumnReader.coder, data);
        return new Builder(new Bitmap[buffer.count()], buffer);
    }

    @Override
    public void setup()
    {
    }

    @Override
    public int getRowCount()
    {
        return getBuffer().count();
    }

    @Override
    public VectorCursor createVectorCursor(int size)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(int[] positions, int offset, int size, VectorCursor dst)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(int offset, int size, VectorCursor dst)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(int[] positions, int offset, int size, BlockBuilder dst)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(int offset, int size, BlockBuilder dst)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
    }

    public static class Builder
            implements CStoreColumnReader.Builder
    {
        private final Bitmap[] data;

        private final BinaryOffsetVector<Bitmap> buffer;

        public Builder(Bitmap[] data, BinaryOffsetVector<Bitmap> buffer)
        {
            this.data = data;
            this.buffer = buffer;
        }

        @Override
        public BitmapColumnReader duplicate()
        {
            return new BitmapColumnReader(buffer.duplicate(), data);
        }
    }
}
