package org.apache.cstore.project;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.operator.Work;
import com.facebook.presto.operator.project.SelectedPositions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import io.airlift.compress.Decompressor;
import org.apache.cstore.bitmap.Bitmap;
import org.apache.cstore.bitmap.BitmapIterator;
import org.apache.cstore.coder.CompressFactory;
import org.apache.cstore.column.BitmapColumnReader;
import org.apache.cstore.column.CStoreColumnLoader;
import org.apache.cstore.column.CStoreColumnReader;
import org.apache.cstore.column.ConstantDoubleCursor;
import org.apache.cstore.column.DoubleCursor;
import org.apache.cstore.column.LongCursor;
import org.apache.cstore.column.StringCursor;
import org.apache.cstore.column.StringEncodedColumnReader;
import org.apache.cstore.column.VectorCursor;
import org.apache.cstore.dictionary.StringDictionary;
import org.apache.cstore.projection.DoubleMinusCall;
import org.apache.cstore.projection.DoubleMultipleCall;
import org.apache.cstore.projection.DoublePlusCall;
import org.apache.cstore.projection.ScalarCall;
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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 2)
@Warmup(iterations = 15)
@Measurement(iterations = 15)
public class VectorizeProjectionBenchmark
{
    private static final Logger log = Logger.get(VectorizeProjectionBenchmark.class);

    private static final String tablePath = "presto-cstore/sample-data/tpch/lineitem";
    private static final CStoreColumnLoader readerFactory = new CStoreColumnLoader();
    private static final String compressType = TpchTableGenerator.compressType;
    private static final int vectorSize = 1024;
    private static final int rowCount = 6001215;
    private static final int pageSize = TpchTableGenerator.pageSize;
    private final Decompressor decompressor = CompressFactory.INSTANCE.getDecompressor(compressType);

    private final CStoreColumnReader.Builder supplierkeyColumnReader = readerFactory.openLongZipReader(tablePath, "l_supplierkey", BigintType.BIGINT,
            rowCount, pageSize, decompressor);
    private final CStoreColumnReader.Builder extendedpriceColumnReader = readerFactory.openDoubleZipReader(tablePath, "l_extendedprice", DoubleType.DOUBLE,
            rowCount, pageSize, decompressor);
    private final CStoreColumnReader.Builder taxColumnReader = readerFactory.openDoubleZipReader(tablePath, "l_tax", DoubleType.DOUBLE,
            rowCount, pageSize, decompressor);
    private final CStoreColumnReader.Builder discountColumnReader = readerFactory.openDoubleZipReader(tablePath, "l_discount", DoubleType.DOUBLE,
            rowCount, pageSize, decompressor);
    private final CStoreColumnReader.Builder quantityColumnReader = readerFactory.openDoubleZipReader(tablePath, "l_quantity", DoubleType.DOUBLE,
            rowCount, pageSize, decompressor);

    private final BitmapColumnReader.Builder index = readerFactory.openBitmapReader(tablePath, "l_returnflag");
    private final StringEncodedColumnReader.Builder returnflagColumnReader = readerFactory.openStringReader(rowCount, TpchTableGenerator.pageSize, decompressor, tablePath, "l_returnflag", VarcharType.VARCHAR);
    private final StringEncodedColumnReader.Builder statusColumnReader = readerFactory.openStringReader(rowCount, TpchTableGenerator.pageSize, decompressor, tablePath, "l_status", VarcharType.VARCHAR);

    @Test
    @Benchmark
    public void testVectorizeProject()
    {
        StringEncodedColumnReader flagColumnReader = this.returnflagColumnReader.build();
        StringDictionary dictionary = flagColumnReader.getDictionary();
        int id = dictionary.encodeId("A");
        Bitmap index = this.index.build().readObject(id);
        StringEncodedColumnReader statusColumnReader = this.statusColumnReader.build();
        //linestatus, returnflag, supplierkey, quantity, extendedprice, discount, tax
        List<CStoreColumnReader> columnReaders = ImmutableList.of(statusColumnReader, flagColumnReader, supplierkeyColumnReader.build(),
                quantityColumnReader.build(), extendedpriceColumnReader.build(), discountColumnReader.build(), taxColumnReader.build());

        columnReaders.forEach(CStoreColumnReader::setup);

        VectorCursor constVector1 = new ConstantDoubleCursor(1.0, vectorSize);
        List<VectorCursor> cursors = ImmutableList.of(
                new StringCursor(new int[vectorSize], statusColumnReader.getDictionaryValue()), //channel-0 = linestatus
                new StringCursor(new int[vectorSize], flagColumnReader.getDictionaryValue()), //channel-1 = returnflag
                new LongCursor(new long[vectorSize]), //channel-2 = supplierkey
                new DoubleCursor(new long[vectorSize]), //channel-3 = quantity
                new DoubleCursor(new long[vectorSize]), //channel-4 = extendedprice
                new DoubleCursor(new long[vectorSize]), //channel-5 = discount
                new DoubleCursor(new long[vectorSize]), //channel-6 = tax
                constVector1, //channel-7 = constant 1.0
                new DoubleCursor(new long[vectorSize]), //channel-8 =  1 - discount
                new DoubleCursor(new long[vectorSize]), //channel-9 =  (1 - discount) * extendedprice
                new DoubleCursor(new long[vectorSize]), //channel-10 =  (1 + tax)
                new DoubleCursor(new long[vectorSize])); //channel-11 =  extendedprice * (1 - discount) * (1 + tax)

        List<ScalarCall> projectionCalls = ImmutableList.of(new DoubleMinusCall(7, 5, 8),
                new DoubleMultipleCall(8, 4, 9),
                new DoublePlusCall(7, 6, 10),
                new DoubleMultipleCall(9, 10, 11));

        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];

        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            for (int i = 0; i < columnReaders.size(); i++) {
                VectorCursor cursor = cursors.get(i);
                CStoreColumnReader columnReader = columnReaders.get(i);
                columnReader.read(positions, 0, count, cursor);
            }
            for (int i = 0; i < projectionCalls.size(); i++) {
                projectionCalls.get(i).process(cursors, count);
            }

            Block[] inputBlocks = new Block[cursors.size() - 2];
            for (int i = 0, j = 0; i < cursors.size(); i++) {
                if (i != 8 && i != 9) {
                    inputBlocks[j++] = cursors.get(i).toBlock(count);
                }
            }
            Page page = new Page(count, inputBlocks);
        }
        columnReaders.forEach(CStoreColumnReader::close);
    }

    @Test
    @Benchmark
    public void testCodeGenerateProject()
    {
        StringEncodedColumnReader flagColumnReader = this.returnflagColumnReader.build();
        StringDictionary dictionary = flagColumnReader.getDictionary();
        int id = dictionary.encodeId("A");
        Bitmap index = this.index.build().readObject(id);
        StringEncodedColumnReader statusColumnReader = this.statusColumnReader.build();
        //extendedprice, discount, tax, linestatus, returnflag, supplierkey, quantity
        List<CStoreColumnReader> columnReaders = ImmutableList.of(extendedpriceColumnReader.build(), discountColumnReader.build(), taxColumnReader.build(),
                statusColumnReader, flagColumnReader, supplierkeyColumnReader.build(), quantityColumnReader.build());

        columnReaders.forEach(CStoreColumnReader::setup);

        List<VectorCursor> cursors = columnReaders.stream().map(reader -> reader.createVectorCursor(vectorSize)).collect(Collectors.toList());

        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];

        long projectTimeNanos = 0;
        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            for (int i = 0; i < columnReaders.size(); i++) {
                VectorCursor cursor = cursors.get(i);
                CStoreColumnReader columnReader = columnReaders.get(i);
                columnReader.read(positions, 0, count, cursor);
            }
            Block[] inputBlocks = cursors.stream().map(cursor -> cursor.toBlock(count)).toArray(Block[]::new);
            Page inputPage = new Page(count, inputBlocks);

            Stopwatch stopwatch = Stopwatch.createStarted();
            ImmutableList<BlockBuilder> builders = ImmutableList.<BlockBuilder>builder()
                    .add(DoubleType.DOUBLE.createBlockBuilder(null, count))
                    .add(DoubleType.DOUBLE.createBlockBuilder(null, count)).build();

            Work<List<Block>> work = new PageProjectionWorkPresto(builders, null, inputPage, SelectedPositions.positionsRange(0, count));
            boolean done = work.process();
            if (done) {
                List<Block> result = work.getResult();
                Block[] outBlocks = new Block[inputBlocks.length + builders.size()];
                System.arraycopy(inputBlocks, 0, outBlocks, 0, inputBlocks.length);
                for (int i = inputBlocks.length; i < outBlocks.length; i++) {
                    outBlocks[i] = result.get(i - inputBlocks.length);
                }
                Page outPage = new Page(count, outBlocks);
            }
            else {
                throw new IllegalStateException();
            }
            projectTimeNanos += stopwatch.elapsed(TimeUnit.NANOSECONDS);
        }
        columnReaders.forEach(CStoreColumnReader::close);
        log.info("project cost %d ms", TimeUnit.NANOSECONDS.toMillis(projectTimeNanos));
    }

    public static void main(String[] args)
            throws RunnerException, IOException
    {
        Options options = new OptionsBuilder()
                .warmupMode(WarmupMode.INDI)
                .include(VectorizeProjectionBenchmark.class.getCanonicalName() + "\\.test.*")
                .build();

        new Runner(options).run();
    }
}
