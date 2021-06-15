package github.cstore.column;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.google.common.base.Stopwatch;
import io.airlift.compress.Decompressor;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.concurrent.TimeUnit;

public abstract class AbstractColumnZipReader
        extends AbstractColumnReader
{
    private static final Logger log = Logger.get(AbstractColumnZipReader.class);

    private final int rowCount;
    private final Decompressor decompressor;
    private final Type type;
    private final int pageValueCount;
    private PageReader pageReader;

    private long decompressTimeNanos;
    private long readTimeNanos;
    private long readCount;
    private long readPageCount;
    private final boolean nullable;

    public AbstractColumnZipReader(int rowCount,
            BinaryOffsetVector<ByteBuffer> chunks,
            Decompressor decompressor,
            int pageValueCount,
            Type type,
            boolean nullable)
    {
        super(chunks);
        this.rowCount = rowCount;
        this.decompressor = decompressor;
        this.type = type;
        this.pageValueCount = pageValueCount;
        this.pageReader = nextPageReader(0, 0, ByteBuffer.wrap(new byte[0]), -1);
        this.nullable = nullable;
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
    public final int read(int[] positions, int offset, int size, VectorCursor dst)
    {
        int totalReadCount = 0;
        while (totalReadCount < size) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            int position = positions[offset + totalReadCount];
            int pageNum = position / pageValueCount;
            if (pageNum != pageReader.pageNum) {
                loadPage(pageNum);
            }
            decompressTimeNanos += stopwatch.elapsed(TimeUnit.NANOSECONDS);
            stopwatch = Stopwatch.createStarted();
            int curReadCount = pageReader.read(positions, totalReadCount + offset, size - totalReadCount, dst, totalReadCount);
            totalReadCount += curReadCount;
            readTimeNanos += stopwatch.elapsed(TimeUnit.NANOSECONDS);
            readPageCount++;
        }
        readCount++;
        return size;
    }

    @Override
    public final int read(int offset, int size, VectorCursor dst)
    {
        int i = 0;
        while (i < size) {
            int position = i + offset;
            int pageNum = position / pageValueCount;
            if (pageNum != pageReader.pageNum) {
                loadPage(pageNum);
            }
            int j = Math.min(pageReader.end - position, size - i);
            pageReader.read(position, j, dst, i);
            i += j;
        }
        return size;
    }

    private void loadPage(int pageNum)
    {
        ByteBuffer chunk = chunks.readBuffer(pageNum);
        int decompressSize = chunk.getInt();
        ByteBuffer decompressBuffer = pageReader.rawBuffer;
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
        pageReader = getPageReader(valueOffset, valueOffset + valueCount, decompressBuffer, pageNum);
    }

    private PageReader getPageReader(int offset, int end, ByteBuffer buffer, int pageNum)
    {
        if (nullable) {
            int nullByteSize = buffer.getInt();
            if (nullByteSize > 0) {
                ByteBuffer nullBuffer = buffer.slice();
                nullBuffer.limit(nullByteSize);
                buffer.position(buffer.position() + nullByteSize);
                ByteBuffer rawBuffer = buffer.slice();
                return nextNullablePageReader(offset, end, rawBuffer, nullBuffer, pageNum);
            }
        }
        ByteBuffer rawBuffer = buffer.slice();
        return nextPageReader(offset, end, rawBuffer, pageNum);
    }

    protected abstract PageReader nextPageReader(int offset, int end, ByteBuffer buffer, int pageNum);

    protected abstract NullablePageReader nextNullablePageReader(int offset, int end, ByteBuffer rawBuffer,
            ByteBuffer nullBuffer, int pageNum);

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

        public abstract int read(int[] positions, int offset, int size, VectorCursor dst, int dstStart);

        public abstract int read(int offset, int size, VectorCursor dst, int dstOffset);
    }

    protected abstract static class NullablePageReader
            extends PageReader
    {
        protected final ByteBuffer nullBuffer;
        private final BitSet nullBitmap;

        protected NullablePageReader(int offset, int end, ByteBuffer rawBuffer, ByteBuffer nullBuffer, int pageNum)
        {
            super(offset, end, rawBuffer, pageNum);
            this.nullBuffer = nullBuffer;
            this.nullBitmap = BitSet.valueOf(nullBuffer);
        }

        public boolean isNull(int position)
        {
            return nullBitmap.get(position);
        }
    }

    @Override
    public void close()
    {
        log.info("decompress cost %d ms, read cost %d ms, read call %d times, page read %d times",
                TimeUnit.NANOSECONDS.toMillis(decompressTimeNanos),
                TimeUnit.NANOSECONDS.toMillis(readTimeNanos), readCount, readPageCount);
    }

    @Deprecated
    protected abstract int getValueSize();
}
