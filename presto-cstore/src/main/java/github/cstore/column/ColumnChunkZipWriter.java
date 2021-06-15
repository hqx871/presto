package github.cstore.column;

import com.facebook.presto.common.block.Block;
import github.cstore.coder.BufferCoder;
import github.cstore.io.StreamWriter;
import github.cstore.io.StreamWriterFactory;
import io.airlift.compress.Compressor;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ColumnChunkZipWriter<T>
        extends AbstractColumnWriter<T>
{
    private final int maxPageRowCount;
    private final Compressor compressor;
    private final CStoreColumnWriter<T> delegate;
    private ByteBuffer compressBuffer;
    private final BinaryOffsetColumnWriter<ByteBuffer> chunkWriter;

    public ColumnChunkZipWriter(String name,
            int maxPageRowCount,
            Compressor compressor,
            StreamWriter streamWriter,
            StreamWriterFactory writerFactory,
            CStoreColumnWriter<T> delegate,
            boolean delete)
    {
        super(name, streamWriter, delete);
        this.maxPageRowCount = maxPageRowCount;
        this.compressor = compressor;
        this.delegate = delegate;
        this.chunkWriter = new BinaryOffsetColumnWriter<>(name, streamWriter, writerFactory, BufferCoder.BYTE_BUFFER, delete);
    }

    @Override
    protected int doWrite(T value)
    {
        if (delegate.getRowCount() >= maxPageRowCount) {
            flushDataPage();
        }
        return delegate.write(value);
    }

    private void flushDataPage()
    {
        ByteBuffer page = delegate.mapBuffer();
        int size = page.remaining();
        int compressBufferSize = compressor.maxCompressedLength(size) + Integer.BYTES;
        if (compressBuffer == null || compressBuffer.capacity() < compressBufferSize) {
            compressBuffer = ByteBuffer.allocateDirect(compressBufferSize);
        }
        else {
            compressBuffer.clear();
        }
        compressBuffer.putInt(size);
        compressor.compress(page, compressBuffer);
        compressBuffer.flip();
        chunkWriter.write(compressBuffer);
        delegate.reset();
    }

    @Override
    protected void doFlush()
            throws IOException
    {
        if (delegate.getRowCount() > 0) {
            flushDataPage();
        }
        chunkWriter.flush();
        super.doFlush();
    }

    @Override
    public void close()
            throws IOException
    {
        flush();
        delegate.close();
        chunkWriter.close();
        super.close();
    }

    @Override
    public void reset()
    {
        chunkWriter.reset();
        delegate.reset();
        super.reset();
    }

    @Override
    public int writeNull()
    {
        if (delegate.getRowCount() >= maxPageRowCount) {
            flushDataPage();
        }
        return delegate.writeNull();
    }

    @Override
    public T readValue(Block src, int position)
    {
        return delegate.readValue(src, position);
    }
}
