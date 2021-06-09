package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;
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

    @Override
    public Short readBlock(Block src, int position)
    {
        return src.getShort(position);
    }
}
