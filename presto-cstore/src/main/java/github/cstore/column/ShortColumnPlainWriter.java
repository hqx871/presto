package github.cstore.column;

import com.facebook.presto.common.block.Block;
import github.cstore.io.StreamWriter;

public class ShortColumnPlainWriter
        extends AbstractColumnWriter<Short>
{
    public ShortColumnPlainWriter(String name, StreamWriter streamWriter, boolean delete)
    {
        super(name, streamWriter, delete);
    }

    @Override
    public int doWrite(Short value)
    {
        valueStreamWriter.putShort(value);
        return Short.BYTES;
    }

    @Override
    public int writeNull()
    {
        return doWrite((short) 0);
    }

    @Override
    public Short readValue(Block src, int position)
    {
        if (src.isNull(position)) {
            return null;
        }
        return src.getShort(position);
    }
}
