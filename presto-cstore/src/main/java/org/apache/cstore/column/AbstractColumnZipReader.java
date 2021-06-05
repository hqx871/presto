package org.apache.cstore.column;

import com.facebook.presto.common.type.Type;
import com.google.common.base.Stopwatch;
import io.airlift.compress.Decompressor;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public abstract class AbstractColumnZipReader
        extends AbstractColumnReader
{
    private final int rowCount;
    private final Decompressor decompressor;
    private final int pageSize;
    private final Type type;
    private final int pageValueCount;
    private PageReader pageReader;

    private long decompressTimeNanos;
    private long readTimeNanos;
    private long readCount;

    public AbstractColumnZipReader(int rowCount,
            BinaryOffsetVector<ByteBuffer> chunks,
            Decompressor decompressor,
            int pageSize,
            Type type)
    {
        super(chunks);
        this.pageSize = pageSize;
        this.rowCount = rowCount;
        this.decompressor = decompressor;
        this.type = type;
        this.pageValueCount = pageSize / getValueSize();
        this.pageReader = nextPageReader(0, 0, ByteBuffer.wrap(new byte[0]), -1);
    }

    @Override
    public void setup()
    {
    }

    @Override
    public int getRowCount()
    {
        return rowCount;
    }

    @Override
    public int read(int[] positions, int offset, int size, VectorCursor dst)
    {
        int i = 0;
        while (i < size) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            int position = positions[offset + i];
            int pageNum = position / pageValueCount;
            if (pageNum != pageReader.pageNum) {
                loadPage(pageNum);
            }
            decompressTimeNanos += stopwatch.elapsed(TimeUnit.NANOSECONDS);
            stopwatch = Stopwatch.createStarted();

            int j = i;
            while (j < size) {
                if (positions[offset + j] >= pageReader.end) {
                    break;
                }
                j++;
            }
            pageReader.read(positions, i + offset, j - i, dst, i);
            i = j;
            readTimeNanos += stopwatch.elapsed(TimeUnit.NANOSECONDS);
            readCount++;
        }
        return size;
    }

    @Override
    public int read(int offset, int size, VectorCursor dst)
    {
        int i = 0;
        while (i < size) {
            int position = i + offset;
            int pageNum = position / pageValueCount;
            if (pageNum != pageReader.pageNum) {
                loadPage(pageNum);
            }
            int j = Math.min(pageReader.end, size);
            pageReader.read(i, j - i, dst, i);
            i = j;
        }
        return size;
    }

    protected final void loadPage(int pageNum)
    {
        ByteBuffer chunk = chunks.readBuffer(pageNum);
        int decompressSize = chunk.getInt();
        ByteBuffer compressed = chunk.slice();
        ByteBuffer decompressed = pageReader.rawBuffer;
        if (decompressed.capacity() > decompressSize) {
            decompressed.clear();
        }
        else {
            decompressed = ByteBuffer.allocateDirect(decompressSize);
        }
        decompressor.decompress(compressed, decompressed);
        decompressed.flip();
        int valueOffset = pageNum * pageValueCount;
        int valueCount = Math.min(pageValueCount, rowCount - valueOffset);
        pageReader = nextPageReader(valueOffset, valueOffset + valueCount, decompressed, pageNum);
    }

    protected abstract PageReader nextPageReader(int offset, int end, ByteBuffer buffer, int pageNum);

    protected abstract static class PageReader
    {
        protected final int offset;
        protected final int end;
        protected final ByteBuffer rawBuffer;
        protected final int pageNum;

        protected PageReader(int offset, int end, ByteBuffer rawBuffer, int pageNum)
        {
            this.offset = offset;
            this.end = end;
            this.rawBuffer = rawBuffer;
            this.pageNum = pageNum;
        }

        public abstract void read(int[] positions, int offset, int size, VectorCursor dst, int dstStart);

        public abstract int read(int offset, int size, VectorCursor dst, int dstOffset);
    }

    @Override
    public void close()
    {
    }

    protected abstract int getValueSize();
}
