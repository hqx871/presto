package org.apache.cstore.project;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.operator.Work;
import com.facebook.presto.operator.project.SelectedPositions;
import com.google.common.collect.ImmutableList;
import io.airlift.compress.Decompressor;
import org.apache.cstore.bitmap.Bitmap;
import org.apache.cstore.bitmap.BitmapIterator;
import org.apache.cstore.coder.CompressFactory;
import org.apache.cstore.column.CStoreColumnLoader;
import org.apache.cstore.column.CStoreColumnReader;
import org.apache.cstore.column.VectorCursor;
import org.apache.cstore.tpch.TpchTableGenerator;
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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
    private static final CStoreColumnLoader readerFactory = new CStoreColumnLoader();
    private static final String compressType = TpchTableGenerator.compressType;
    private static final int rowCount = 6001215;
    private static final int pageSize = TpchTableGenerator.pageSize;
    private final Decompressor decompressor = CompressFactory.INSTANCE.getDecompressor(compressType);

    private final CStoreColumnReader.Builder extendedpriceColumnReader = readerFactory.openDoubleZipReader(tablePath, "l_extendedprice", DoubleType.DOUBLE,
            rowCount, pageSize, decompressor);
    private final CStoreColumnReader.Builder taxColumnReader = readerFactory.openDoubleZipReader(tablePath, "l_tax", DoubleType.DOUBLE,
            rowCount, pageSize, decompressor);
    private final CStoreColumnReader.Builder discountColumnReader = readerFactory.openDoubleZipReader(tablePath, "l_discount", DoubleType.DOUBLE,
            rowCount, pageSize, decompressor);
    private final Bitmap index = readerFactory.openBitmapReader(tablePath, "l_returnflag").duplicate().readObject(1);
    private static final int vectorSize = 1024;
    //private final List<CStoreColumnReader> columnReaders = ImmutableList.of(extendedpriceColumnReader, discountColumnReader, taxColumnReader);

    @Test
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
        List<CStoreColumnReader> columnReaders = ImmutableList.of(extendedpriceColumnReader.duplicate(),
                discountColumnReader.duplicate(),
                taxColumnReader.duplicate());

        List<VectorCursor> cursors = columnReaders.stream().map(columnReader -> columnReader.createVectorCursor(vectorSize))
                .collect(Collectors.toList());

        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];

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