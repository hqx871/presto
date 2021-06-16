package github.cstore.column;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.google.common.base.Stopwatch;
import github.cstore.coder.BufferCoder;
import io.airlift.compress.Decompressor;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public final class ColumnChunkZipReader
        implements CStoreColumnReader
{
    private final Logger log = Logger.get(getClass());

    protected final BinaryOffsetColumnReader<ByteBuffer> chunks;
    private final int rowCount;
    private final Decompressor decompressor;
    private final Type type;
    private final int pageValueCount;
    private AbstractColumnPlainReader pageReaderBuilder;

    private long decompressTimeNanos;
    private long readTimeNanos;
    private long readCount;
    private long readPageCount;
    private final boolean nullable;
    private final AbstractColumnPlainReader.Factory plainReaderFactory;
    private int pageNum;

    public ColumnChunkZipReader(int rowCount,
            int pageValueCount,
            BinaryOffsetColumnReader<ByteBuffer> chunks,
            Decompressor decompressor,
            Type type,
            boolean nullable,
            AbstractColumnPlainReader.Factory plainReaderFactory)
    {
        this.pageValueCount = pageValueCount;
        this.chunks = chunks;
        this.rowCount = rowCount;
        this.decompressor = decompressor;
        this.type = type;

        this.plainReaderFactory = plainReaderFactory;
        this.pageReaderBuilder = getPageReader(0, 0, ByteBuffer.wrap(new byte[0]));
        this.nullable = nullable;
        this.pageNum = -1;
    }

    @Override
    public void setup()
    {
    }

    @Override
    public final int getRowCount()
    {
        return rowCount;
    }

    @Override
    public VectorCursor createVectorCursor(int size)
    {
        return pageReaderBuilder.createVectorCursor(size);
    }

    @Override
    public final int read(int[] positions, int offset, int size, VectorCursor dst, int dstOffset)
    {
        int totalReadCount = 0;
        while (totalReadCount < size) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            int position = positions[offset + totalReadCount];
            int pageNum = position / pageValueCount;
            if (pageNum != this.pageNum) {
                loadPage(pageNum);
            }
            decompressTimeNanos += stopwatch.elapsed(TimeUnit.NANOSECONDS);
            stopwatch = Stopwatch.createStarted();
            int curReadCount = pageReaderBuilder.read(positions, totalReadCount + offset,
                    size - totalReadCount, dst, dstOffset + totalReadCount);
            totalReadCount += curReadCount;
            readTimeNanos += stopwatch.elapsed(TimeUnit.NANOSECONDS);
            readPageCount++;
        }
        readCount++;
        return size;
    }

    @Override
    public final int read(int offset, int size, VectorCursor dst, int dstOffset)
    {
        int i = 0;
        while (i < size) {
            int position = i + offset;
            int pageNum = position / pageValueCount;
            if (pageNum != this.pageNum) {
                loadPage(pageNum);
            }
            int j = Math.min(pageReaderBuilder.getEnd() - position, size - i);
            pageReaderBuilder.read(position, j, dst, i + dstOffset);
            i += j;
        }
        return size;
    }

    private void loadPage(int pageNum)
    {
        ByteBuffer chunk = chunks.readByteBuffer(pageNum);
        int decompressSize = chunk.getInt();
        ByteBuffer decompressBuffer = pageReaderBuilder.getRawBuffer();
        if (decompressBuffer.capacity() >= decompressSize) {
            decompressBuffer.clear();
            decompressBuffer.limit(decompressSize);
        }
        else {
            decompressBuffer = ByteBuffer.allocateDirect(decompressSize);
        }
        decompressor.decompress(chunk, decompressBuffer);
        decompressBuffer.flip();
        int valueOffset = pageNum * pageValueCount;
        int valueCount = Math.min(pageValueCount, rowCount - valueOffset);
        pageReaderBuilder = getPageReader(valueOffset, valueOffset + valueCount, decompressBuffer);
        this.pageNum = pageNum;
        pageReaderBuilder.setup();
    }

    private AbstractColumnPlainReader getPageReader(int offset, int end, ByteBuffer buffer)
    {
        if (nullable) {
            int nullByteSize = buffer.getInt();
            if (nullByteSize > 0) {
                ByteBuffer nullBuffer = buffer.slice();
                nullBuffer.limit(nullByteSize);
                buffer.position(buffer.position() + nullByteSize);
                ByteBuffer rawBuffer = buffer.slice();
                return plainReaderFactory.createNullableReader(offset, end, rawBuffer, nullBuffer);
            }
        }
        ByteBuffer rawBuffer = buffer.slice();
        return plainReaderFactory.createPlainReader(offset, end, rawBuffer);
    }

    @Override
    public void close()
    {
        log.info("decompress cost %d ms, read cost %d ms, read call %d times, page read %d times",
                TimeUnit.NANOSECONDS.toMillis(decompressTimeNanos),
                TimeUnit.NANOSECONDS.toMillis(readTimeNanos), readCount, readPageCount);
    }

    public static class Builder
            implements CStoreColumnReader.Builder
    {
        private final int rowCount;
        private final int pageRowCount;
        private final BinaryOffsetColumnReader<ByteBuffer> chunks;
        private final Decompressor decompressor;
        private final Type type;
        private final boolean nullable;
        private final AbstractColumnPlainReader.Factory delegate;

        public Builder(int rowCount, ByteBuffer buffer, Decompressor decompressor,
                Type type, boolean nullable, AbstractColumnPlainReader.Factory delegate)
        {
            this.rowCount = rowCount;
            this.pageRowCount = buffer.getInt();
            this.chunks = BinaryOffsetColumnReader.decode(BufferCoder.BYTE_BUFFER, buffer.slice());
            this.decompressor = decompressor;
            this.type = type;
            this.nullable = nullable;
            this.delegate = delegate;
        }

        @Override
        public CStoreColumnReader build()
        {
            return new ColumnChunkZipReader(rowCount, pageRowCount, chunks.duplicate(), decompressor, type, nullable, delegate);
        }
    }

    public static Builder newBuilder(int rowCount, ByteBuffer buffer, Decompressor decompressor,
            Type type, boolean nullable, AbstractColumnPlainReader.Factory delegate)
    {
        return new Builder(rowCount, buffer, decompressor, type, nullable, delegate);
    }
}
