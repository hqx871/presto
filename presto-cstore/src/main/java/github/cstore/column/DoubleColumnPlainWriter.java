package github.cstore.column;

import com.facebook.presto.common.block.Block;
import github.cstore.io.StreamWriter;

public class DoubleColumnPlainWriter
        extends AbstractColumnWriter<Double>
{
    public DoubleColumnPlainWriter(String name, StreamWriter streamWriter, boolean delete)
    {
        super(name, streamWriter, delete);
    }

    @Override
    public int doWrite(Double value)
    {
        valueStreamWriter.putDouble(value);
        return Double.BYTES;
    }

    @Override
    public int writeNull()
    {
        return doWrite(0D);
    }

    @Override
    public Double readValue(Block src, int position)
    {
        if (src.isNull(position)) {
            return null;
        }
        return Double.longBitsToDouble(src.getLong(position));
    }
}
