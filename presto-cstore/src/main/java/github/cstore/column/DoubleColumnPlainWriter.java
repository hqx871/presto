package github.cstore.column;

import com.facebook.presto.common.block.Block;
import github.cstore.io.StreamWriterFactory;

public class DoubleColumnPlainWriter
        extends AbstractColumnWriter<Double>
{
    public DoubleColumnPlainWriter(String name, StreamWriterFactory writerFactory, boolean delete)
    {
        super(name, writerFactory.createWriter(name + ".bin", delete), delete);
    }

    @Override
    public int write(Double value)
    {
        streamWriter.putDouble(value);
        return Double.BYTES;
    }

    @Override
    public Double readBlockValue(Block src, int position)
    {
        return Double.longBitsToDouble(src.getLong(position));
    }
}
