package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;
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

    @Override
    public Integer readBlock(Block src, int position)
    {
        return src.getInt(position);
    }
}
