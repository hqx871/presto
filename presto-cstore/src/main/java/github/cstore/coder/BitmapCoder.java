package github.cstore.coder;

import github.cstore.bitmap.Bitmap;
import github.cstore.bitmap.RoaringBitmapAdapter;

import java.nio.ByteBuffer;

public class BitmapCoder
        implements BufferCoder<Bitmap>
{
    private final int capacity;

    public BitmapCoder(int capacity)
    {
        this.capacity = capacity;
    }

    @Override
    public Bitmap decode(ByteBuffer data)
    {
        return RoaringBitmapAdapter.open(capacity, data);
    }

    @Override
    public ByteBuffer encode(Bitmap object)
    {
        return object.encode();
    }
}
