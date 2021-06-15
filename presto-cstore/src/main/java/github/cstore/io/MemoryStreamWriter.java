package github.cstore.io;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class MemoryStreamWriter
        extends DataOutputStreamWriter
{
    private ByteArrayOutputStream buffer;
    private final boolean delete;

    public MemoryStreamWriter(ByteArrayOutputStream buffer, boolean delete)
    {
        super(new DataOutputStream(buffer));
        this.buffer = buffer;
        this.delete = delete;
    }

    @Override
    public void delete()
    {
        if (delete && buffer != null) {
            buffer = null;
        }
        buffer = null;
    }

    @Override
    public ByteBuffer toByteBuffer()
    {
        try {
            buffer.flush();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return ByteBuffer.wrap(buffer.toByteArray());
    }

    @Override
    public void reset()
    {
        buffer.reset();
    }
}
