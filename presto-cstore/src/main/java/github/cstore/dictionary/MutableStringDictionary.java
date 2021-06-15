package github.cstore.dictionary;

import github.cstore.coder.BufferCoder;
import github.cstore.column.BinaryOffsetColumnWriter;
import github.cstore.column.CStoreColumnWriter;
import github.cstore.io.StreamWriter;
import github.cstore.io.StreamWriterFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public abstract class MutableStringDictionary
        extends StringDictionary
{
    public abstract int encode(String value);

    @Override
    public boolean isSort()
    {
        return false;
    }

    public abstract int[] sortValue();

    public abstract List<String> getNonNullValues();

    public final int writeSst(StreamWriter output, String name, StreamWriterFactory writerFactory)
            throws IOException
    {
        output.putByte((byte) 0);
        CStoreColumnWriter<String> valueWriter = new BinaryOffsetColumnWriter<>(name, writerFactory.createWriter(name + ".bin", true), writerFactory, BufferCoder.UTF8, true);
        for (String val : getNonNullValues()) {
            valueWriter.write(val);
        }
        ByteBuffer valueBuffer = valueWriter.mapBuffer();
        int valueSize = valueBuffer.remaining();
        output.putByteBuffer(valueBuffer);

        valueWriter.close();
        return Byte.BYTES + valueSize;
    }
}
