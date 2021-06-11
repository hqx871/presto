package github.cstore.column;

import github.cstore.io.StreamWriterFactory;

import java.io.IOException;

public class ByteColumnRleWriter
        extends AbstractColumnWriter<Byte>
{
    private byte currentValue;
    private byte currentLength;

    public ByteColumnRleWriter(String name, StreamWriterFactory writerFactory, int count, boolean delete)
    {
        super(name, writerFactory.createWriter(name + ".bin", delete), delete);
        streamWriter.putInt(count);
    }

    @Override
    public int write(Byte value)
    {
        if (currentLength == 0) {
            currentLength = 1;
            currentValue = value;
            return 0;
        }
        else if (currentValue == value) {
            currentLength++;
            if (currentLength == Byte.MAX_VALUE) {
                streamWriter.putByte(currentLength);
                streamWriter.putByte(currentValue);

                currentLength = 0;
            }
            return 0;
        }
        else {
            streamWriter.putByte(currentLength);
            streamWriter.putByte(currentValue);

            int size = 2 * currentLength;
            currentValue = value;
            currentLength = 1;
            return size;
        }
    }

    @Override
    protected void doFlush()
            throws IOException
    {
        if (currentLength > 0) {
            streamWriter.putByte(currentLength);
            streamWriter.putByte(currentValue);
        }
        streamWriter.flush();
    }
}
