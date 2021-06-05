package org.apache.cstore.column;

import org.apache.cstore.io.VectorWriterFactory;

public class DoubleColumnPlainWriter
        extends AbstractColumnWriter<Double>
{
    public DoubleColumnPlainWriter(VectorWriterFactory writerFactory, boolean delete)
    {
        super(writerFactory, delete);
    }

    @Override
    public int write(Double value)
    {
        streamWriter.putDouble(value);
        return Double.BYTES;
    }
}
