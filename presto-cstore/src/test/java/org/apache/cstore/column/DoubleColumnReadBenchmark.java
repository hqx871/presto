package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.LongArrayBlockBuilder;
import com.facebook.presto.common.type.DoubleType;
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
import java.nio.DoubleBuffer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(2)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class DoubleColumnReadBenchmark
{
    private static final String tablePath = "presto-cstore/sample-data/tpch/lineitem";
    private static final String columnName = "l_tax";
    private static final CStoreColumnLoader readerFactory = new CStoreColumnLoader();
    private final DoubleColumnPlainReader.Builder columnReader = readerFactory.openDoubleReader(tablePath, columnName, DoubleType.DOUBLE);
    private final Bitmap index = readerFactory.openBitmapReader(tablePath, "l_returnflag").duplicate().readObject(1);
    private static final int vectorSize = 1024;

    @Benchmark
    public void testWriteToBlockBuilder()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        DoubleColumnPlainReader columnReader = this.columnReader.duplicate();
        columnReader.setup();
        DoubleBuffer buffer = columnReader.getDataBuffer();
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            BlockBuilder blockBuilder = new LongArrayBlockBuilder(null, vectorSize);
            for (int i = 0; i < count; i++) {
                blockBuilder.writeLong(Double.doubleToLongBits(buffer.get(positions[i]))).closeEntry();
            }
        }
    }

    @Benchmark
    public void testWriteToArray()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        DoubleColumnPlainReader columnReader = this.columnReader.duplicate();
        columnReader.setup();
        DoubleBuffer buffer = columnReader.getDataBuffer();
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            double[] array = new double[vectorSize];
            for (int i = 0; i < count; i++) {
                array[i] = buffer.get(positions[i]);
            }
        }
    }

    @Benchmark
    public void testWriteToVectorCursor()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        DoubleColumnPlainReader columnReader = this.columnReader.duplicate();
        columnReader.setup();
        DoubleBuffer buffer = columnReader.getDataBuffer();
        VectorCursor cursor = columnReader.createVectorCursor(vectorSize);
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            for (int i = 0; i < count; i++) {
                cursor.writeDouble(i, buffer.get(positions[i]));
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException, IOException
    {
        Options options = new OptionsBuilder()
                .warmupMode(WarmupMode.INDI)
                .include(DoubleColumnReadBenchmark.class.getCanonicalName() + "\\.test.*")
                .build();

        new Runner(options).run();
    }
}
