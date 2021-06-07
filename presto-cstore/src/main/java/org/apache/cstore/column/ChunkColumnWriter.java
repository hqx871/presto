package org.apache.cstore.column;

import io.airlift.compress.Compressor;
import org.apache.cstore.io.CStoreColumnWriter;
import org.apache.cstore.io.VectorWriterFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public class ChunkColumnWriter<T>
        extends AbstractColumnWriter<T>
{
    private final int pageSize;
    private final Compressor compressor;
    private final CStoreColumnWriter<T> delegate;

    public ChunkColumnWriter(int pageSize,
            Compressor compressor,
            VectorWriterFactory writerFactory,
            CStoreColumnWriter<T> delegate,
            boolean delete)
    {
        super(writerFactory, delete);
        this.pageSize = pageSize;
        this.compressor = compressor;
        this.delegate = delegate;
    }

    @Override
    public int write(T value)
    {
        return delegate.write(value);
    }

    @Override
    public void doFlush()
            throws IOException
    {
        delegate.flush();
        MappedByteBuffer mapFile = delegate.mapFile();

        ByteBuffer compressBuffer = ByteBuffer.allocate(compressor.maxCompressedLength(pageSize));
        int pageCount = (int) Math.ceil(1.0 * mapFile.limit() / pageSize);
        int[] offsets = new int[pageCount + 1];
        for (int i = 0; i < pageCount; i++) {
            mapFile.position(i * pageSize);
            ByteBuffer page = mapFile.slice();
            int size = Math.min(pageSize, page.remaining());
            page.limit(size);
            compressBuffer.clear();
            compressor.compress(page, compressBuffer);
            compressBuffer.flip();
            streamWriter.putInt(size);
            streamWriter.putByteBuffer(compressBuffer);
            offsets[i + 1] = offsets[i] + compressBuffer.limit() + Integer.BYTES;
        }
        for (int i = 0; i < offsets.length; i++) {
            streamWriter.putInt(offsets[i]);
        }
        streamWriter.putInt(offsets[pageCount]);
        streamWriter.putInt(Integer.BYTES * offsets.length);
        streamWriter.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        flush();
        delegate.close();
        super.close();
    }
}
