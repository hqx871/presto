package github.cstore.column;

import com.facebook.presto.common.block.Block;
import github.cstore.io.StreamWriter;

public class LongColumnPlainWriter
        extends AbstractColumnWriter<Long>
{
    public LongColumnPlainWriter(String name, StreamWriter streamWriter, boolean delete)
    {
        super(name, streamWriter, delete);
    }

    @Override
    public int doWrite(Long value)
    {
        valueStreamWriter.putLong(value);
        return Long.BYTES;
    }

    @Override
    public int writeNull()
    {
        return doWrite(0L);
    }

    @Override
    public Long readValue(Block src, int position)
    {
        return src.getLong(position);
    }
}
