package github.cstore.bitmap;

import java.nio.ByteBuffer;

public interface Bitmap
{
    ByteBuffer encode();

    Bitmap not();

    BitmapIterator iterator();

    <T> T unwrap();

    int getCapacity();
}
