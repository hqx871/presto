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
    private final DoubleColumnReader extendedpriceColumnReader = QueryBenchmarkTool.mapDoubleColumnReader(tablePath + "/l_extendedprice.bin");
    private final DoubleColumnReader taxColumnReader = QueryBenchmarkTool.mapDoubleColumnReader(tablePath + "/l_tax.bin");
    private final DoubleColumnReader discountColumnReader = QueryBenchmarkTool.mapDoubleColumnReader(tablePath + "/l_discount.bin");
    private final Bitmap index = QueryBenchmarkTool.mapBitmapIndex(tablePath + "/l_returnflag.bitmap", 1);
    private static final int vectorSize = 1024;
    private final List<DoubleColumnReader> columnReaders = ImmutableList.of(extendedpriceColumnReader, discountColumnReader, taxColumnReader);

    @Benchmark
    public void testProjectionNonNull()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];

        List<VectorCursor> cursors = ImmutableList.of(
                extendedpriceColumnReader.createVectorCursor(vectorSize),
                discountColumnReader.createVectorCursor(vectorSize),
                taxColumnReader.createVectorCursor(vectorSize)
        );
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            List<Block> blocks = new ArrayList<>();
            for (int i = 0; i < cursors.size(); i++) {
                VectorCursor cursor = cursors.get(i);
                CStoreColumnReader columnReader = columnReaders.get(i);
                columnReader.read(positions, 0, count, cursor);
                blocks.add(cursor.toBlock(count));
            }
            Page page = new Page(count, blocks.toArray(new Block[0]));
            Block extendedpriceBlock = page.getBlock(0);
            Block taxBlock = page.getBlock(1);
            Block discountBlock = page.getBlock(2);
            List<BlockBuilder> builders = ImmutableList.of(
                    DoubleType.DOUBLE.createBlockBuilder(null, count),
                    DoubleType.DOUBLE.createBlockBuilder(null, count)
            );
            BlockBuilder builder0 = builders.get(0);
            BlockBuilder builder1 = builders.get(1);
            for (int i = 0; i < count; i++) {
                //no null value
                double v0 = DoubleType.DOUBLE.getDouble(extendedpriceBlock, i) * (1 - DoubleType.DOUBLE.getDouble(discountBlock, i));
                DoubleType.DOUBLE.writeDouble(builder0, v0);
                double v1 = v0 * (1 + DoubleType.DOUBLE.getDouble(taxBlock, i));
                DoubleType.DOUBLE.writeDouble(builder1, v1);
            }
            List<Block> result = ImmutableList.<Block>builder()
                    .add(builder0.build())
                    .add(builder1.build())
                    .build();
        }
    }

    @Benchmark
    public void testProjectionNullable()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];

        List<VectorCursor> cursors = ImmutableList.of(
                extendedpriceColumnReader.createVectorCursor(vectorSize),
                discountColumnReader.createVectorCursor(vectorSize),
                taxColumnReader.createVectorCursor(vectorSize)
        );
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            List<Block> blocks = new ArrayList<>();
            for (int i = 0; i < cursors.size(); i++) {
                VectorCursor cursor = cursors.get(i);
                CStoreColumnReader columnReader = columnReaders.get(i);
                columnReader.read(positions, 0, count, cursor);
                blocks.add(cursor.toBlock(count));
            }
            Page page = new Page(count, blocks.toArray(new Block[0]));
            Block extendedpriceBlock = page.getBlock(0);
            Block taxBlock = page.getBlock(1);
            Block discountBlock = page.getBlock(2);
            List<BlockBuilder> builders = ImmutableList.of(
                    DoubleType.DOUBLE.createBlockBuilder(null, count),
                    DoubleType.DOUBLE.createBlockBuilder(null, count)
            );
            BlockBuilder builder0 = builders.get(0);
            BlockBuilder builder1 = builders.get(1);
            for (int i = 0; i < count; i++) {
                //handle null value
                if (extendedpriceBlock.isNull(i) || discountBlock.isNull(i)) {
                    builder0.appendNull();
                    builder1.appendNull();
                }
                else {
                    double v0 = DoubleType.DOUBLE.getDouble(extendedpriceBlock, i) * (1 - DoubleType.DOUBLE.getDouble(discountBlock, i));
                    DoubleType.DOUBLE.writeDouble(builder0, v0);
                    if (taxBlock.isNull(i)) {
                        builder1.appendNull();
                    }
                    else {
                        double v1 = v0 * (1 + DoubleType.DOUBLE.getDouble(taxBlock, i));
                        DoubleType.DOUBLE.writeDouble(builder1, v1);
                    }
                }
            }

            List<Block> result = ImmutableList.<Block>builder()
                    .add(builder0.build())
                    .add(builder1.build())
                    .build();
        }
    }

    @Benchmark
    public void testProjectionUnbox()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];

        List<VectorCursor> cursors = ImmutableList.of(
                extendedpriceColumnReader.createVectorCursor(vectorSize),
                discountColumnReader.createVectorCursor(vectorSize),
                taxColumnReader.createVectorCursor(vectorSize)
        );
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            List<Block> blocks = new ArrayList<>();
            for (int i = 0; i < cursors.size(); i++) {
                VectorCursor cursor = cursors.get(i);
                CStoreColumnReader columnReader = columnReaders.get(i);
                columnReader.read(positions, 0, count, cursor);
                blocks.add(cursor.toBlock(count));
            }
            Page page = new Page(count, blocks.toArray(new Block[0]));
            Block extendedpriceBlock = page.getBlock(0);
            Block taxBlock = page.getBlock(1);
            Block discountBlock = page.getBlock(2);
            List<BlockBuilder> builders = ImmutableList.of(
                    DoubleType.DOUBLE.createBlockBuilder(null, count),
                    DoubleType.DOUBLE.createBlockBuilder(null, count)
            );
            BlockBuilder builder0 = builders.get(0);
            BlockBuilder builder1 = builders.get(1);
            for (int i = 0; i < count; i++) {
                //handle null value
                Double extendprice = extendedpriceBlock.isNull(i) ? null : DoubleType.DOUBLE.getDouble(extendedpriceBlock, i);
                Double v0 = null;
                if (extendprice != null && !discountBlock.isNull(i)) {
                    v0 = extendprice * (1 - DoubleType.DOUBLE.getDouble(discountBlock, i));
                }
                if (v0 == null) {
                    builder0.appendNull();
                }
                else {
                    DoubleType.DOUBLE.writeDouble(builder0, v0);
                }
                Double v1 = null;
                if (v0 != null && !taxBlock.isNull(i)) {
                    v1 = v0 * (1 + DoubleType.DOUBLE.getDouble(taxBlock, i));
                }

                if (v1 == null) {
                    builder1.appendNull();
                }
                else {
                    DoubleType.DOUBLE.writeDouble(builder1, v1);
                }
            }

            List<Block> result = ImmutableList.<Block>builder()
                    .add(builder0.build())
                    .add(builder1.build())
                    .build();
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

    @Benchmark
    public void testProjectWorkDemo()
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];

        List<VectorCursor> cursors = ImmutableList.of(
                extendedpriceColumnReader.createVectorCursor(vectorSize),
                discountColumnReader.createVectorCursor(vectorSize),
                taxColumnReader.createVectorCursor(vectorSize)
        );
        while (iterator.hasNext()) {
            int count = iterator.next(positions);

            List<Block> blocks = new ArrayList<>();
            for (int i = 0; i < cursors.size(); i++) {
                VectorCursor cursor = cursors.get(i);
                CStoreColumnReader columnReader = columnReaders.get(i);
                columnReader.read(positions, 0, count, cursor);
                blocks.add(cursor.toBlock(count));
            }
            Page page = new Page(count, blocks.toArray(new Block[0]));

            ImmutableList.Builder<BlockBuilder> builders = ImmutableList.builder();
            builders.add(DoubleType.DOUBLE.createBlockBuilder(null, count));
            builders.add(DoubleType.DOUBLE.createBlockBuilder(null, count));

            PageProjectionWorkDemo projectionWork = new PageProjectionWorkDemo(builders.build(), null, page, SelectedPositions.positionsRange(0, count));
            if (projectionWork.process()) {
                List<Block> result = projectionWork.getResult();
            }
            else {
                throw new IllegalStateException();
            }
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
