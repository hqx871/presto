package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.LongArrayBlockBuilder;
import com.facebook.presto.common.type.BigintType;
import io.airlift.compress.Decompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import org.apache.cstore.bitmap.Bitmap;
import org.apache.cstore.bitmap.BitmapIterator;
import org.apache.cstore.coder.CoderFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.WarmupMode;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.LongBuffer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(2)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class LongColumnReadBenchmark
{
    private static final String tablePath = "presto-cstore/sample-data/tpch/lineitem";
    private static final String columnName = "l_partkey";
    private static final String compressType = "lz4";
    private static final CStoreColumnReaderFactory readerFactory = new CStoreColumnReaderFactory();
    private final Decompressor decompressor = CoderFactory.INSTANCE.getDecompressor(compressType);
    private final LongColumnPlainReader columnReader = readerFactory.openLongReader(tablePath, columnName, BigintType.BIGINT);
    private final LongColumnZipReader columnZipReader = readerFactory.openLongZipReader(tablePath, columnName, BigintType.BIGINT,
            6001215, 64 << 10, decompressor);

    private final Bitmap index = readerFactory.openBitmapReader(tablePath, "l_returnflag").readObject(1);
    private static final int vectorSize = 1024;

    @Benchmark
    public void testWriteToLongBlockBuilder()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        LongBuffer buffer = columnReader.getDataBuffer();
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            BlockBuilder blockBuilder = new LongArrayBlockBuilder(null, vectorSize);
            for (int i = 0; i < count; i++) {
                blockBuilder.writeLong(buffer.get(positions[i])).closeEntry();
            }
        }
    }

    @Benchmark
    public void testWriteToLongArray()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        LongBuffer buffer = columnReader.getDataBuffer();
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            long[] array = new long[vectorSize];
            for (int i = 0; i < count; i++) {
                array[i] = buffer.get(positions[i]);
            }
        }
    }

    @Benchmark
    public void testWriteToLongArrayImpl()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        LongBuffer buffer = columnReader.getDataBuffer();
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            LongArray blockBuilder = new LongArrayImpl(vectorSize);
            for (int i = 0; i < count; i++) {
                blockBuilder.writeLong(buffer.get(positions[i]));
            }
        }
    }

    @Benchmark
    public void testWriteToLongArrayImplUnchecked()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        LongBuffer buffer = columnReader.getDataBuffer();
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            LongArray blockBuilder = new LongArrayImpl(vectorSize);
            for (int i = 0; i < count; i++) {
                blockBuilder.writeLongUnchecked(buffer.get(positions[i]));
            }
        }
    }

    @Benchmark
    public void testWriteToLongArrayFinal()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        LongBuffer buffer = columnReader.getDataBuffer();
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            LongArray blockBuilder = new LongArrayFinal(vectorSize);
            for (int i = 0; i < count; i++) {
                blockBuilder.writeLong(buffer.get(positions[i]));
            }
        }
    }

    @Test
    @Benchmark
    public void testWriteToLongVectorCursor()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        LongBuffer buffer = columnReader.getDataBuffer();
        columnReader.setup();
        VectorCursor cursor = columnReader.createVectorCursor(vectorSize);
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            for (int i = 0; i < count; i++) {
                cursor.writeLong(i, buffer.get(positions[i]));
            }
        }
    }

    @Test
    @Benchmark
    public void testWriteZipToLongVectorCursor()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        columnZipReader.setup();
        VectorCursor cursor = columnZipReader.createVectorCursor(vectorSize);
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            columnZipReader.read(positions, 0, count, cursor);
        }
        columnZipReader.close();
    }

    @Benchmark
    public void testWriteToLongArrayFinalUnchecked()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        LongBuffer buffer = columnReader.getDataBuffer();
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            LongArray blockBuilder = new LongArrayFinal(vectorSize);
            for (int i = 0; i < count; i++) {
                blockBuilder.writeLongUnchecked(buffer.get(positions[i]));
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException, IOException
    {
        Options options = new OptionsBuilder()
                .warmupMode(WarmupMode.INDI)
                .include(LongColumnReadBenchmark.class.getCanonicalName() + "\\.test.*")
                .build();

        new Runner(options).run();
    }
}
