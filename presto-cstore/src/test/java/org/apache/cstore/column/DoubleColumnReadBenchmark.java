package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.LongArrayBlockBuilder;
import com.facebook.presto.common.type.DoubleType;
import io.airlift.compress.Decompressor;
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
    private final Decompressor decompressor = CoderFactory.INSTANCE.getDecompressor("lz4");
    private final DoubleColumnPlainReader.Builder columnReader = readerFactory.openDoubleReader(tablePath, columnName, DoubleType.DOUBLE);
    private final Bitmap index = readerFactory.openBitmapReader(tablePath, "l_returnflag").duplicate().readObject(1);
    private static final int vectorSize = 1024;
    private final DoubleColumnZipReader.Builder columnZipReader = readerFactory.openDoubleZipReader(tablePath, columnName, DoubleType.DOUBLE,
            6001215, 64 << 10, decompressor);

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

    @Test
    @Benchmark
    public void testWriteZipToVectorCursor()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        DoubleColumnZipReader columnZipReader = this.columnZipReader.duplicate();
        columnZipReader.setup();
        VectorCursor cursor = columnZipReader.createVectorCursor(vectorSize);
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            columnZipReader.read(positions, 0, count, cursor);
        }
        columnZipReader.close();
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
