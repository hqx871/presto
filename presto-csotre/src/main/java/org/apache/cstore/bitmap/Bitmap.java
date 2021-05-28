package org.apache.cstore.bitmap;

import java.nio.ByteBuffer;
import java.util.List;

public interface Bitmap
{
    static Bitmap create(ByteBuffer buffer)
    {
        return RoaringBitmapAdapter.open(buffer);
    }

    ByteBuffer encode();

    BitmapFactory factory();

    Bitmap and(Bitmap other);

    boolean get(int position);

    boolean[] toBoolArray(int position, int length);

    int sizeInByte();

    Bitmap and(List<Bitmap> others);

    Bitmap or(List<Bitmap> others);

    Bitmap or(Bitmap other);

    BitmapIterator iterator();

    <T> T unwrap();
}
