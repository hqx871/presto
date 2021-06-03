package org.apache.cstore.project;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.operator.Work;
import com.facebook.presto.operator.project.SelectedPositions;
import com.google.common.collect.ImmutableList;
import org.apache.cstore.bitmap.Bitmap;
import org.apache.cstore.bitmap.BitmapIterator;
import org.apache.cstore.column.CStoreColumnReader;
import org.apache.cstore.column.CStoreColumnReaderFactory;
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
import java.util.ArrayList;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 2)
@Warmup(iterations = 15)
@Measurement(iterations = 15)
public class PageProjectionBenchmark
{
    private static final String tablePath = "presto-cstore/sample-data/tpch/lineitem";
    private static final CStoreColumnReaderFactory readerFactory = new CStoreColumnReaderFactory();

    private final DoubleColumnReader extendedpriceColumnReader = readerFactory.openDoubleReader(tablePath, "l_extendedprice", DoubleType.DOUBLE);
    private final DoubleColumnReader taxColumnReader = readerFactory.openDoubleReader(tablePath, "l_tax", DoubleType.DOUBLE);
    private final DoubleColumnReader discountColumnReader = readerFactory.openDoubleReader(tablePath, "l_discount", DoubleType.DOUBLE);
    private final Bitmap index = readerFactory.openBitmapReader(tablePath, "l_returnflag").readObject(1);
    private static final int vectorSize = 1024;
    private final List<DoubleColumnReader> columnReaders = ImmutableList.of(extendedpriceColumnReader, discountColumnReader, taxColumnReader);

    @Benchmark
    public void testProjectionNonNull()
    {
        runProjectWork(PageProjectionWorkNonNull::new);
    }

    @Benchmark
    public void testProjectionNullable()
    {
        runProjectWork(PageProjectionWorkNullable::new);
    }

    @Benchmark
    public void testProjectionUnbox()
    {
        runProjectWork(PageProjectionWorkUnbox::new);
    }

    @Benchmark
    public void testProjectWorkPresto()
    {
        runProjectWork(PageProjectionWorkPresto::new);
    }

    private void runProjectWork(PageProjectionFactory projectionWorkFactory)
    {
        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];

        List<VectorCursor> cursors = ImmutableList.of(
                extendedpriceColumnReader.createVectorCursor(vectorSize),
                discountColumnReader.createVectorCursor(vectorSize),
                taxColumnReader.createVectorCursor(vectorSize));
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

            Work<List<Block>> projectionWork = projectionWorkFactory.create(builders.build(), null, page, SelectedPositions.positionsRange(0, count));
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
                .build();

        new Runner(options).run();
    }
}
