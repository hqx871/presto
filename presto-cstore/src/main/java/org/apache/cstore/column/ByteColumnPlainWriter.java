package org.apache.cstore.column;

import org.apache.cstore.io.VectorWriterFactory;

public class ByteColumnPlainWriter
        extends AbstractColumnWriter<Byte>
{
    public ByteColumnPlainWriter(VectorWriterFactory writerFactory, boolean delete)
    {
        super(writerFactory, delete);
    }

    @Override
    public int write(Byte value)
    {
        streamWriter.putByte(value);
        return Byte.BYTES;
    }
}
