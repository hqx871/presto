package org.apache.cstore.bitmap;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class RoaringBitmapFactory
        implements BitmapFactory
{
    @Override
    public Bitmap create(int[] value, int offset, int length)
    {
        MutableRoaringBitmap mutable = new MutableRoaringBitmap();
        for (int i = 0; i < length; i++) {
            mutable.add(value[offset + i]);
        }
        return new RoaringBitmapAdapter(mutable.toImmutableRoaringBitmap());
    }
}
