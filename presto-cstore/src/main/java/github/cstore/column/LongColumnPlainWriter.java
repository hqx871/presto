package github.cstore.column;

import com.facebook.presto.common.block.Block;
import github.cstore.io.StreamWriterFactory;

public class LongColumnPlainWriter
        extends AbstractColumnWriter<Long>
{
    public LongColumnPlainWriter(String name, StreamWriterFactory writerFactory, boolean delete)
    {
        super(name, writerFactory.createWriter(name + ".bin", delete), delete);
    }

    @Override
    public int write(Long value)
    {
        streamWriter.putLong(value);
        return Long.BYTES;
    }

    @Override
    public Long readBlockValue(Block src, int position)
    {
        return src.getLong(position);
    }
}
