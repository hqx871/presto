package github.cstore.io;

import java.io.ByteArrayOutputStream;

public class MemoryStreamWriterFactory
        implements StreamWriterFactory
{
    @Override
    public StreamWriter createWriter(String name, boolean clean)
    {
        return new MemoryStreamWriter(new ByteArrayOutputStream(), clean);
    }
}
