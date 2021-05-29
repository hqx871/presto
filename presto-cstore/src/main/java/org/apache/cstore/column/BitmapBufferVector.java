package org.apache.cstore.column;

import org.apache.cstore.bitmap.Bitmap;
import org.apache.cstore.coder.BufferCoder;

import java.nio.ByteBuffer;

public class BitmapBufferVector
        extends CacheVector<Bitmap>
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

    public BitmapBufferVector(BinaryOffsetVector<Bitmap> buffer)
    {
        super(buffer, new Bitmap[buffer.count()], coder);
    }
}
