package github.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.cstore.storage.CStoreColumnLoader;
import github.cstore.bitmap.Bitmap;
import github.cstore.bitmap.BitmapIterator;
import github.cstore.coder.CompressFactory;
import github.cstore.dictionary.DictionaryBlockBuilder;
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
    private static final String tablePath = "presto-cstore/sample-data/tpch/lineitem";
    private static final String columnName = "l_status";
    private static final ColumnFileLoader columnFileLoader = new ColumnFileLoader(new File(tablePath));
    private static final CStoreColumnLoader readerFactory = new CStoreColumnLoader();
    private final Decompressor decompressor = CompressFactory.INSTANCE.getDecompressor("lz4");
    private final StringEncodedColumnReader.Builder columnReader = readerFactory.openStringReader(6001215, decompressor,
            columnFileLoader.open(columnName + ".tar"), VarcharType.VARCHAR);
    private final Bitmap index = readerFactory.openBitmapReader(columnFileLoader.open("l_returnflag.bitmap")).build().readObject(1);
    private static final int vectorSize = 1024;

    @Test
    @Benchmark
    public void testWriteToBlockBuilder()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        StringEncodedColumnReader columnReader = this.columnReader.build();
        columnReader.setup();
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            int[] ids = new int[vectorSize];
            BlockBuilder blockBuilder = new DictionaryBlockBuilder(columnReader.getDictionaryValue(), new int[vectorSize], null);
            columnReader.read(positions, 0, count, new IntCursor(ids), 0);
            Block block = blockBuilder.build();
        }
    }

    @Benchmark
    public void testWriteToArray()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        StringEncodedColumnReader columnReader = this.columnReader.build();
        columnReader.setup();
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            int[] ids = new int[vectorSize];
            columnReader.read(positions, 0, count, new IntCursor(ids), 0);
            Block block = new DictionaryBlock(columnReader.getDictionaryValue(), ids);
        }
    }

    @Benchmark
    public void testWriteToVectorCursor()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        StringEncodedColumnReader columnReader = this.columnReader.build();
        columnReader.setup();
        VectorCursor cursor = columnReader.createVectorCursor(vectorSize);
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            int[] ids = new int[vectorSize];
            columnReader.read(positions, 0, count, new IntCursor(ids), 0);
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
