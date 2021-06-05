package org.apache.cstore.column;

import org.apache.cstore.io.VectorWriterFactory;

public class ShortColumnPlainWriter
        extends AbstractColumnWriter<Short>
{
    public ShortColumnPlainWriter(VectorWriterFactory writerFactory, boolean delete)
    {
        super(writerFactory, delete);
    }

    @Override
    public int write(Short value)
    {
        streamWriter.putShort(value);
        return Short.BYTES;
    }
}
