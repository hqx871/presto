package org.apache.cstore.project;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.operator.project.SelectedPositions;
import com.google.common.collect.ImmutableList;
import org.apache.cstore.QueryBenchmarkTool;
import org.apache.cstore.bitmap.Bitmap;
import org.apache.cstore.bitmap.BitmapIterator;
import org.apache.cstore.column.CStoreColumnReader;
import org.apache.cstore.column.DoubleColumnReader;
import org.apache.cstore.column.VectorCursor;
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
import java.util.ArrayList;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 10)
public class PageProjectionBenchmark
{
    private static final String tablePath = "/Users/huangqixiang/tmp/cstore/tpch/lineitem";
    private final DoubleColumnReader taxColumnReader = QueryBenchmarkTool.mapDoubleColumnReader(tablePath + "/l_tax.bin");
    private final DoubleColumnReader discountColumnReader = QueryBenchmarkTool.mapDoubleColumnReader(tablePath + "/l_discount.bin");
    private final Bitmap index = QueryBenchmarkTool.mapBitmapIndex(tablePath + "/l_returnflag.bitmap", 1);
    private static final int vectorSize = 1024;
    private final List<DoubleColumnReader> columnReaders = ImmutableList.of(taxColumnReader, discountColumnReader);

    @Benchmark
    public void testProjectProjectWorkDemo()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];

        List<VectorCursor> cursors = ImmutableList.of(
                taxColumnReader.createVectorCursor(vectorSize),
                discountColumnReader.createVectorCursor(vectorSize)
        );
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            ImmutableList.Builder<BlockBuilder> builders = ImmutableList.builder();
            List<Block> blocks = new ArrayList<>();
            for (int i = 0; i < cursors.size(); i++) {
                VectorCursor cursor = cursors.get(i);
                CStoreColumnReader columnReader = columnReaders.get(i);
                columnReader.read(positions, 0, count, cursor);
                blocks.add(cursor.toBlock(count));
                builders.add(DoubleType.DOUBLE.createBlockBuilder(null, count));
            }
            Page page = new Page(count, blocks.toArray(new Block[0]));
            PageProjectionWorkDemo projectionWork = new PageProjectionWorkDemo(builders.build(), null, page, SelectedPositions.positionsRange(0, count));
            if (projectionWork.process()) {
                List<Block> result = projectionWork.getResult();
            }
            else {
                throw new IllegalStateException();
            }
        }
    }

    @Benchmark
    public void warnUp()
    {
        for (DoubleColumnReader columnReader : columnReaders) {
            columnReader.setup();
            DoubleBuffer buffer = columnReader.getDataBuffer();
            for (int i = 0; i < buffer.limit(); i++) {
                buffer.get(i);
            }
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
                .include(PageProjectionBenchmark.class.getCanonicalName() + "\\.test.*")
                .includeWarmup(PageProjectionBenchmark.class.getCanonicalName() + "\\.warnUp")
                .build();

        new Runner(options).run();
    }
}
