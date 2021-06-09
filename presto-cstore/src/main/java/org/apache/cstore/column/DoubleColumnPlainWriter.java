package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;
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

    @Override
    public Double readBlock(Block src, int position)
    {
        return Double.longBitsToDouble(src.getLong(position));
    }
}
