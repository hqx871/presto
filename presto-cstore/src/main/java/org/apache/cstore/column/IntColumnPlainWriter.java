package org.apache.cstore.column;

import org.apache.cstore.io.VectorWriterFactory;

public class IntColumnPlainWriter
        extends AbstractColumnWriter<Integer>
{
    public IntColumnPlainWriter(VectorWriterFactory writerFactory, boolean delete)
    {
        super(writerFactory, delete);
    }

    @Override
    public int write(Integer value)
    {
        streamWriter.putInt(value);
        return Integer.BYTES;
    }
}
