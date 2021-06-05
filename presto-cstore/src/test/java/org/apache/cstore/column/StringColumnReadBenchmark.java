package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.type.VarcharType;
import io.airlift.compress.zstd.ZstdDecompressor;
import org.apache.cstore.bitmap.Bitmap;
import org.apache.cstore.bitmap.BitmapIterator;
import org.apache.cstore.dictionary.DictionaryBlockBuilder;
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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(2)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class StringColumnReadBenchmark
{
    private static final String tablePath = "sample-data/tpch/lineitem";
    private static final String columnName = "l_status";
    private static final CStoreColumnReaderFactory readerFactory = new CStoreColumnReaderFactory();

    private final StringEncodedColumnReader columnReader = readerFactory.openStringReader(6001215, 64 << 10, new ZstdDecompressor(), tablePath, columnName, VarcharType.VARCHAR);
    private final Bitmap index = readerFactory.openBitmapReader(tablePath, "l_returnflag").readObject(1);
    private static final int vectorSize = 1024;

    @Test
    @Benchmark
    public void testWriteToBlockBuilder()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        columnReader.setup();
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            int[] ids = new int[vectorSize];
            BlockBuilder blockBuilder = new DictionaryBlockBuilder(columnReader.getDictionaryValue(), new int[vectorSize], null);
            columnReader.read(positions, 0, count, new IntCursor(ids));
            Block block = blockBuilder.build();
        }
    }

    @Benchmark
    public void testWriteToArray()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        columnReader.setup();
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            int[] ids = new int[vectorSize];
            columnReader.read(positions, 0, count, new IntCursor(ids));
            Block block = new DictionaryBlock(columnReader.getDictionaryValue(), ids);
        }
    }

    @Benchmark
    public void testWriteToVectorCursor()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        columnReader.setup();
        VectorCursor cursor = columnReader.createVectorCursor(vectorSize);
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            int[] ids = new int[vectorSize];
            columnReader.read(positions, 0, count, new IntCursor(ids));
            Block block = cursor.toBlock(count);
        }
    }

    public static void main(String[] args)
            throws RunnerException, IOException
    {
        Options options = new OptionsBuilder()
                .warmupMode(WarmupMode.INDI)
                .include(StringColumnReadBenchmark.class.getCanonicalName() + "\\.test.*")
                .build();

        new Runner(options).run();
    }
}
