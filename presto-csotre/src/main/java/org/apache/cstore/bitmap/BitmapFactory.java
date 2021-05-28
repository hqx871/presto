package org.apache.cstore.bitmap;

public interface BitmapFactory
{
    Bitmap create(int[] value, int offset, int length);
}
