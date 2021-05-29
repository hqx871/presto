package org.apache.cstore.bitmap;

public interface BitmapIterator
{
    boolean hasNext();

    int next(int[] mask);
}
