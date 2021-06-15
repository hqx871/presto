package github.cstore.column;

import com.facebook.presto.common.block.Block;
import github.cstore.io.StreamWriter;

public class ByteColumnPlainWriter
        extends AbstractColumnWriter<Byte>
{
    public ByteColumnPlainWriter(String name, StreamWriter streamWriter, boolean delete)
    {
        super(name, streamWriter, delete);
    }

    @Override
    public int doWrite(Byte value)
    {
        valueStreamWriter.putByte(value);
        return Byte.BYTES;
    }

    @Override
    public Byte readValue(Block src, int position)
    {
        if (src.isNull(position)) {
            return null;
        }
        return src.getByte(position);
    }

    @Override
    public int writeNull()
    {
        return doWrite((byte) 0);
    }
}
