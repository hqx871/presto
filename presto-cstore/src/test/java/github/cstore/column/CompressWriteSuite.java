package github.cstore.column;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.cstore.storage.CStoreColumnLoader;
import github.cstore.io.FileStreamWriterFactory;
import github.cstore.io.StreamWriterFactory;
import io.airlift.compress.zstd.ZstdCompressor;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.DoubleBuffer;
import java.nio.LongBuffer;

public class CompressWriteSuite
{
    private static final String tablePath = "sample-data/tpch/lineitem";
    private static final CStoreColumnLoader readerFactory = new CStoreColumnLoader();
    private static final int rowCount = 6001215;
    private static final int pageRowCount = 64 << 10;
    private static final String type = "bin";
    private static final ColumnFileLoader columnFileLoader = new ColumnFileLoader(new File(tablePath));

    @Test
    public void testWriteDoubleColumn()
            throws IOException
    {
        String columnName = "l_tax";
        DoubleColumnPlainReader columnReader = readerFactory.openDoublePlainReader(columnFileLoader.open(columnName + ".bin"), DoubleType.DOUBLE)
                .build();
        StreamWriterFactory writerFactory = new FileStreamWriterFactory(new File(tablePath));
        ColumnChunkZipWriter<Double> writer = new ColumnChunkZipWriter<>(columnName, pageRowCount,
                new ZstdCompressor(), writerFactory.createWriter(columnName + ".tar", true), writerFactory,
                new DoubleColumnPlainWriter(columnName, writerFactory.createWriter(columnName + ".bin", true), false),
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
        LongColumnPlainReader longColumnReader = readerFactory.openLongPlainReader(columnFileLoader.open(columnName + ".bin"), BigintType.BIGINT)
                .build();
        StreamWriterFactory writerFactory = new FileStreamWriterFactory(new File(tablePath));
        ColumnChunkZipWriter<Long> writer = new ColumnChunkZipWriter<>(columnName, pageRowCount,
                new ZstdCompressor(), writerFactory.createWriter(columnName + ".tar", true), writerFactory,
                new LongColumnPlainWriter(columnName, writerFactory.createWriter(columnName + ".plain", true), false), false);
        LongBuffer buffer = longColumnReader.getDataBuffer();
        for (int i = 0; i < buffer.limit(); i++) {
            writer.write(buffer.get(i));
        }
        writer.close();
    }
}