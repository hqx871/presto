package github.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.LongArrayBlockBuilder;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.cstore.storage.CStoreColumnLoader;
import github.cstore.bitmap.Bitmap;
import github.cstore.bitmap.BitmapIterator;
import github.cstore.coder.CompressFactory;
import github.cstore.util.ExecutorManager;
import io.airlift.compress.Decompressor;
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

import java.io.File;
import java.io.IOException;
import java.nio.DoubleBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(2)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class DoubleColumnReadBenchmark
{
    private static final String tablePath = "presto-cstore/sample-data/tpch/lineitem";
    private static final String columnName = "l_tax";
    private static final ColumnFileLoader columnFileLoader = new ColumnFileLoader(new File(tablePath));
    private static final CStoreColumnLoader readerFactory = new CStoreColumnLoader();
    private final Decompressor decompressor = CompressFactory.INSTANCE.getDecompressor("lz4");
    private final DoubleColumnPlainReader.Builder columnReader = readerFactory.openDoublePlainReader(columnFileLoader.open(columnName + ".bin"), DoubleType.DOUBLE);
    private final Bitmap index = readerFactory.openBitmapReader(columnFileLoader.open("l_returnflag.bitmap")).build().readObject(1);
    private static final int vectorSize = 1024;
    private final DoubleColumnZipReader.Builder columnZipReader = readerFactory.openDoubleZipReader(columnFileLoader.open(columnName + ".tar"), DoubleType.DOUBLE,
            6001215, 64 << 10, decompressor);

    @Benchmark
    public void testWriteToBlockBuilder()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        DoubleColumnPlainReader columnReader = this.columnReader.build();
        columnReader.setup();
        DoubleBuffer buffer = columnReader.getDataBuffer();
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            BlockBuilder blockBuilder = new LongArrayBlockBuilder(null, vectorSize);
            for (int i = 0; i < count; i++) {
                blockBuilder.writeLong(Double.doubleToLongBits(buffer.get(positions[i]))).closeEntry();
            }
        }
        columnReader.close();
    }

    @Benchmark
    public void testWriteToVectorCursor()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        DoubleColumnPlainReader columnReader = this.columnReader.build();
        columnReader.setup();
        DoubleBuffer buffer = columnReader.getDataBuffer();
        VectorCursor cursor = columnReader.createVectorCursor(vectorSize);
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            for (int i = 0; i < count; i++) {
                cursor.writeDouble(i, buffer.get(positions[i]));
            }
        }
        columnReader.close();
    }

    @Test
    @Benchmark
    public void testWriteZipToVectorCursor()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        DoubleColumnZipReader columnZipReader = this.columnZipReader.build();
        columnZipReader.setup();
        VectorCursor cursor = columnZipReader.createVectorCursor(vectorSize);
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            columnZipReader.read(positions, 0, count, cursor);
        }
        columnZipReader.close();
    }

    @Test
    //@Benchmark
    public void testMultiThreadWriteZipToVectorCursor()
            throws ExecutionException, InterruptedException
    {
        ExecutorManager executorManager = new ExecutorManager("projection-%d");
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        DoubleColumnZipReader columnZipReader = this.columnZipReader.build();
        columnZipReader.setup();
        VectorCursor cursor = columnZipReader.createVectorCursor(vectorSize);
        while (iterator.hasNext()) {
            Future<?> future = executorManager.getExecutor().submit(new Runnable()
            {
                @Override
                public void run()
                {
                    int count = iterator.next(positions);
                    columnZipReader.read(positions, 0, count, cursor);
                }
            });
            future.get();
        }
        columnZipReader.close();
        executorManager.getExecutor().shutdownNow();
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