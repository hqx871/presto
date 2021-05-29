package org.apache.cstore.bitmap;

import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RoaringBitmapAdapter
        implements Bitmap
{
    private final ImmutableRoaringBitmap bitmap;

    public RoaringBitmapAdapter(ImmutableRoaringBitmap bitmap)
    {
        this.bitmap = bitmap;
    }

    public static RoaringBitmapAdapter open(ByteBuffer buffer)
    {
        return new RoaringBitmapAdapter(new ImmutableRoaringBitmap(buffer));
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
    public BitmapFactory factory()
    {
        return new RoaringBitmapFactory();
    }

    @Override
    public Bitmap and(Bitmap other)
    {
        MutableRoaringBitmap tmp = bitmap.toMutableRoaringBitmap();
        tmp.and(other.unwrap());
        return new RoaringBitmapAdapter(tmp.toImmutableRoaringBitmap());
    }

    @Override
    public boolean get(int position)
    {
        return bitmap.contains(position);
    }

    @Override
    public boolean[] toBoolArray(int position, int length)
    {
        boolean[] bools = new boolean[length];
        for (int i = 0; i < length; i++) {
            bools[i] = bitmap.contains(position + i);
        }
        return bools;
    }

    @Override
    public int sizeInByte()
    {
        return bitmap.getSizeInBytes();
    }

    @Override
    public Bitmap and(List<Bitmap> others)
    {
        List<ImmutableRoaringBitmap> bitmaps = new ArrayList<>();
        bitmaps.add(bitmap);
        others.forEach(other -> bitmaps.add(other.unwrap()));
        return new RoaringBitmapAdapter(BufferFastAggregation.and(bitmaps.iterator()).toImmutableRoaringBitmap());
    }

    @Override
    public Bitmap or(List<Bitmap> others)
    {
        List<ImmutableRoaringBitmap> bitmaps = new ArrayList<>();
        bitmaps.add(bitmap);
        others.forEach(other -> bitmaps.add(other.unwrap()));
        return new RoaringBitmapAdapter(BufferFastAggregation.or(bitmaps.iterator()).toImmutableRoaringBitmap());
    }

    @Override
    public Bitmap or(Bitmap other)
    {
        MutableRoaringBitmap tmp = bitmap.toMutableRoaringBitmap();
        MutableRoaringBitmap or = other.unwrap();
        tmp.or(or);
        return new RoaringBitmapAdapter(tmp.toImmutableRoaringBitmap());
    }

    @Override
    public Bitmap not()
    {
        throw new UnsupportedOperationException();
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
    public ImmutableRoaringBitmap unwrap()
    {
        return bitmap;
    }
}
