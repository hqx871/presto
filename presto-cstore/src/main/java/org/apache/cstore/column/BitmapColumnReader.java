package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;
import org.apache.cstore.bitmap.Bitmap;
import org.apache.cstore.coder.BufferCoder;

import java.nio.ByteBuffer;

public class BitmapColumnReader
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
        super(buffer, new Bitmap[buffer.count()], coder);
    }

    public static BitmapColumnReader decode(ByteBuffer data)
    {
        BinaryOffsetVector<Bitmap> buffer = BinaryOffsetReader.decode(BitmapColumnReader.coder, data);
        return new BitmapColumnReader(buffer);
    }

    @Override
    public void setup()
    {
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
}
