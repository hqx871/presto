package org.apache.cstore.column;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import io.airlift.compress.zstd.ZstdCompressor;
import org.apache.cstore.io.VectorWriterFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.DoubleBuffer;
import java.nio.LongBuffer;

public class CompressWriteSuite
{
    private static final String tablePath = "sample-data/tpch/lineitem";
    private static final CStoreColumnLoader readerFactory = new CStoreColumnLoader();
    private static final int rowCount = 6001215;
    private static final int pageSize = 64 << 10;
    private static final String type = "bin";

    @Test
    public void testWriteDoubleColumn()
            throws IOException
    {
        String columnName = "l_tax";
        DoubleColumnPlainReader columnReader = readerFactory.openDoubleReader(tablePath, columnName, DoubleType.DOUBLE)
                .duplicate();
        VectorWriterFactory writerFactory = new VectorWriterFactory(tablePath, columnName, "bin");
        ChunkColumnWriter<Double> writer = new ChunkColumnWriter<>(pageSize,
                new ZstdCompressor(), writerFactory,
                new DoubleColumnPlainWriter(writerFactory, false),
                false);
        DoubleBuffer buffer = columnReader.getDataBuffer();
        for (int i = 0; i < buffer.limit(); i++) {
            writer.write(buffer.get(i));
        }
        writer.close();
    }

    @Test
    public void testWriteLongColumn()
            throws IOException
    {
        String columnName = "l_partkey";
        LongColumnPlainReader longColumnReader = readerFactory.openLongReader(tablePath, columnName, BigintType.BIGINT)
                .duplicate();
        VectorWriterFactory writerFactory = new VectorWriterFactory(tablePath, columnName, type);
        ChunkColumnWriter<Long> writer = new ChunkColumnWriter<>(pageSize,
                new ZstdCompressor(), new VectorWriterFactory(tablePath, columnName, type),
                new LongColumnPlainWriter(writerFactory, false), false);
        LongBuffer buffer = longColumnReader.getDataBuffer();
        for (int i = 0; i < buffer.limit(); i++) {
            writer.write(buffer.get(i));
        }
        writer.close();
    }
}
