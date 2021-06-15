package github.cstore.column;

import com.facebook.presto.common.block.Block;
import github.cstore.io.StreamWriter;

public class IntColumnPlainWriter
        extends AbstractColumnWriter<Integer>
{
    public IntColumnPlainWriter(String name, StreamWriter streamWriter, boolean delete)
    {
        super(name, streamWriter, delete);
    }

    @Override
    public int doWrite(Integer value)
    {
        valueStreamWriter.putInt(value);
        return Integer.BYTES;
    }

    @Override
    public int writeNull()
    {
        return doWrite(0);
    }

    @Override
    public Integer readValue(Block src, int position)
    {
        if (src.isNull(position)) {
            return null;
        }
        return src.getInt(position);
    }
}
