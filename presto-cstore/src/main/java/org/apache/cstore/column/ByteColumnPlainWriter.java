package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;
import org.apache.cstore.io.VectorWriterFactory;

public class ByteColumnPlainWriter
        extends AbstractColumnWriter<Byte>
{
    public ByteColumnPlainWriter(VectorWriterFactory writerFactory, boolean delete)
    {
        super(writerFactory, delete);
    }

    @Override
    public int write(Byte value)
    {
        streamWriter.putByte(value);
        return Byte.BYTES;
    }

    @Override
    public Byte readBlock(Block src, int position)
    {
        return src.getByte(position);
    }
}
