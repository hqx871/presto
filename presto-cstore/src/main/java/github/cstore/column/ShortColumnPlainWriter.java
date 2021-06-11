package github.cstore.column;

import com.facebook.presto.common.block.Block;
import github.cstore.io.StreamWriterFactory;

public class ShortColumnPlainWriter
        extends AbstractColumnWriter<Short>
{
    public ShortColumnPlainWriter(String name, StreamWriterFactory writerFactory, boolean delete)
    {
        super(name, writerFactory, delete);
    }

    @Override
    public int write(Short value)
    {
        streamWriter.putShort(value);
        return Short.BYTES;
    }

    @Override
    public Short readBlockValue(Block src, int position)
    {
        return src.getShort(position);
    }
}
