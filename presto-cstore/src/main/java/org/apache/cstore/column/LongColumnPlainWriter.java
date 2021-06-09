package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;
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

    @Override
    public Long readBlock(Block src, int position)
    {
        return src.getLong(position);
    }
}
