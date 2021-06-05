package org.apache.cstore.column;

import org.apache.cstore.io.VectorWriterFactory;

public class LongColumnPlainWriter
        extends AbstractColumnWriter<Long>
{
    public LongColumnPlainWriter(VectorWriterFactory writerFactory, boolean delete)
    {
        super(writerFactory, delete);
    }

    @Override
    public int write(Long value)
    {
        streamWriter.putLong(value);
        return Long.BYTES;
    }
}
