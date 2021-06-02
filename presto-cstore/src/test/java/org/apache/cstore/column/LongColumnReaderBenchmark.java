package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.LongArrayBlockBuilder;
import org.apache.cstore.bitmap.Bitmap;
import org.apache.cstore.bitmap.BitmapIterator;
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

import java.io.IOException;
import java.nio.LongBuffer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 10)
public class LongColumnReaderBenchmark
{
    private static final String tablePath = "/Users/huangqixiang/tmp/cstore/tpch/lineitem";
    private static final String columnName = "l_partkey";
    private static final String indexName = "l_returnflag";
    private final LongColumnReader longColumnReader = ColumnBenchmarkTool.mapLongColumnReader(tablePath + "/" + columnName + ".bin");
    private final Bitmap index = ColumnBenchmarkTool.mapBitmapIndex(tablePath + "/" + indexName + ".bitmap", 1);
    private static final int vectorSize = 1024;

    @Benchmark
    public void testWriteToLongBlockBuilder()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        LongBuffer buffer = longColumnReader.getDataBuffer();
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            BlockBuilder blockBuilder = new LongArrayBlockBuilder(null, vectorSize);
            for (int i = 0; i < count; i++) {
                blockBuilder.writeLong(buffer.get(positions[i]));
            }
        }
    }

    @Benchmark
    public void testWriteToLongArray()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        LongBuffer buffer = longColumnReader.getDataBuffer();
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
        LongBuffer buffer = longColumnReader.getDataBuffer();
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
        LongBuffer buffer = longColumnReader.getDataBuffer();
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
        LongBuffer buffer = longColumnReader.getDataBuffer();
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            LongArray blockBuilder = new LongArrayFinal(vectorSize);
            for (int i = 0; i < count; i++) {
                blockBuilder.writeLong(buffer.get(positions[i]));
            }
        }
    }

    @Benchmark
    public void testWriteToVectorCursor()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        LongBuffer buffer = longColumnReader.getDataBuffer();
        longColumnReader.setup();
        VectorCursor cursor = longColumnReader.createVectorCursor(vectorSize);
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            for (int i = 0; i < count; i++) {
                cursor.writeLong(i, buffer.get(positions[i]));
            }
        }
    }

    @Benchmark
    public void testWriteToLongArrayFinalUnchecked()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        LongBuffer buffer = longColumnReader.getDataBuffer();
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            LongArray blockBuilder = new LongArrayFinal(vectorSize);
            for (int i = 0; i < count; i++) {
                blockBuilder.writeLongUnchecked(buffer.get(positions[i]));
            }
        }
    }

    @Benchmark
    public void warnUp()
    {
        LongBuffer buffer = longColumnReader.getDataBuffer();
        for (int i = 0; i < buffer.limit(); i++) {
            buffer.get(i);
        }
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
        }
    }

    public static void main(String[] args)
            throws RunnerException, IOException
    {
        Options options = new OptionsBuilder()
                .warmupMode(WarmupMode.INDI)
                .include(LongColumnReaderBenchmark.class.getCanonicalName() + "\\.test.*")
                .includeWarmup(LongColumnReaderBenchmark.class.getCanonicalName() + "\\.warnUp")
                .build();

        new Runner(options).run();
    }
}
