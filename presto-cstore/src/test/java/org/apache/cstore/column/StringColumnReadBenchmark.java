package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.type.VarcharType;
import org.apache.cstore.QueryBenchmarkTool;
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
    private static final String tablePath = "/Users/huangqixiang/tmp/cstore/tpch/lineitem";
    private static final String columnName = "l_linestatus";
    private static final String indexName = "l_returnflag";
    private final StringEncodedColumnReader columnReader = new CStoreColumnReaderFactory().openStringReader(tablePath, columnName, VarcharType.VARCHAR);
    private final Bitmap index = QueryBenchmarkTool.mapBitmapIndex(tablePath + "/" + indexName + ".bitmap", 1);
    private static final int vectorSize = 1024;

    @Benchmark
    public void testWriteToBlockBuilder()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        IntVector buffer = columnReader.getDataVector();
        columnReader.setup();
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            BlockBuilder blockBuilder = new DictionaryBlockBuilder(columnReader.getDictionaryValue(), new int[vectorSize], null);
            for (int i = 0; i < count; i++) {
                blockBuilder.writeInt(buffer.readInt(positions[i])).closeEntry();
            }
            Block block = blockBuilder.build();
        }
    }

    @Benchmark
    public void testWriteToArray()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        IntVector buffer = columnReader.getDataVector();
        columnReader.setup();
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            int[] array = new int[vectorSize];
            for (int i = 0; i < count; i++) {
                array[i] = buffer.readInt(positions[i]);
            }
            Block block = new DictionaryBlock(columnReader.getDictionaryValue(), array);
        }
    }

    @Benchmark
    public void testWriteToVectorCursor()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];
        IntVector buffer = columnReader.getDataVector();
        columnReader.setup();
        VectorCursor cursor = columnReader.createVectorCursor(vectorSize);
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            for (int i = 0; i < count; i++) {
                cursor.writeInt(i, (byte) buffer.readInt(positions[i]));
            }
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
