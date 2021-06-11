package github.cstore.column;

import com.facebook.presto.common.block.Block;
import github.cstore.io.StreamWriterFactory;

public class IntColumnPlainWriter
        extends AbstractColumnWriter<Integer>
{
    public IntColumnPlainWriter(String name, StreamWriterFactory writerFactory, boolean delete)
    {
        super(name, writerFactory.createWriter(name + ".bin", delete), delete);
    }

    @Override
    public int write(Integer value)
    {
        streamWriter.putInt(value);
        return Integer.BYTES;
    }

    @Override
    public Integer readBlockValue(Block src, int position)
    {
        return src.getInt(position);
    }
}
