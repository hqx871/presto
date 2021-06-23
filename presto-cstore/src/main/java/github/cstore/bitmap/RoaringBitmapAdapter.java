package github.cstore.bitmap;

import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.nio.ByteBuffer;

public class RoaringBitmapAdapter
        implements Bitmap
{
    private final int capacity;
    private final ImmutableRoaringBitmap bitmap;

    public RoaringBitmapAdapter(int capacity, ImmutableRoaringBitmap bitmap)
    {
        this.capacity = capacity;
        this.bitmap = bitmap;
    }

    public static RoaringBitmapAdapter open(int capacity, ByteBuffer buffer)
    {
        return new RoaringBitmapAdapter(capacity, new ImmutableRoaringBitmap(buffer));
    }

    @Override
    public ByteBuffer encode()
    {
        ByteBuffer buffer = ByteBuffer.allocate(bitmap.serializedSizeInBytes());
        bitmap.serialize(buffer);
        buffer.flip();
        return buffer;
    }

    @Override
    public Bitmap not()
    {
        MutableRoaringBitmap newBitmap = bitmap.toMutableRoaringBitmap();
        newBitmap.flip(0);
        return new RoaringBitmapAdapter(capacity, newBitmap.toImmutableRoaringBitmap());
    }

    @Override
    public BitmapIterator iterator()
    {
        final BatchIterator delegate = bitmap.getBatchIterator();
        return new BitmapIterator()
        {
            @Override
            public boolean hasNext()
            {
                return delegate.hasNext();
            }

            @Override
            public int next(int[] mask)
            {
                if (!delegate.hasNext()) {
                    return 0;
                }
                return delegate.nextBatch(mask);
            }
        };
    }

    @Override
    public int getCapacity()
    {
        return capacity;
    }

    @Override
    public ImmutableRoaringBitmap unwrap()
    {
        return bitmap;
    }
}
