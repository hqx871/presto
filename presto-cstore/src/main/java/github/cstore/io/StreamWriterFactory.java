package github.cstore.io;

public interface StreamWriterFactory
{
    StreamWriter createWriter(String name, boolean clean);
}
