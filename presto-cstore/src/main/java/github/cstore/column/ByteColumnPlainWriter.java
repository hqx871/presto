package github.cstore.column;

import com.facebook.presto.common.block.Block;
import github.cstore.io.StreamWriterFactory;

public class ByteColumnPlainWriter
        extends AbstractColumnWriter<Byte>
{
    public ByteColumnPlainWriter(String name, StreamWriterFactory writerFactory, boolean delete)
    {
        super(name, writerFactory.createWriter(name + ".bin", delete), delete);
    }

    @Override
    public int write(Byte value)
    {
        streamWriter.putByte(value);
        return Byte.BYTES;
    }

    @Override
    public Byte readBlockValue(Block src, int position)
    {
        return src.getByte(position);
    }
}
