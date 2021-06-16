package github.cstore.project;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.cstore.storage.CStoreColumnLoader;
import com.facebook.presto.operator.Work;
import com.facebook.presto.operator.project.SelectedPositions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import github.cstore.bitmap.Bitmap;
import github.cstore.bitmap.BitmapIterator;
import github.cstore.coder.CompressFactory;
import github.cstore.column.BitmapColumnReader;
import github.cstore.column.CStoreColumnReader;
import github.cstore.column.ColumnFileLoader;
import github.cstore.column.ConstantDoubleCursor;
import github.cstore.column.DoubleCursor;
import github.cstore.column.LongCursor;
import github.cstore.column.StringEncodedColumnReader;
import github.cstore.column.StringEncodedCursor;
import github.cstore.column.VectorCursor;
import github.cstore.dictionary.StringDictionary;
import github.cstore.projection.DoubleMinusCall;
import github.cstore.projection.DoubleMultipleCall;
import github.cstore.projection.DoublePlusCall;
import github.cstore.projection.ScalarCall;
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
    private static final String compressType = "lz4";
    private static final int vectorSize = 1024;
    private static final int rowCount = 6001215;
    private static final int pageSize = 64 << 10;
    private final Decompressor decompressor = CompressFactory.INSTANCE.getDecompressor(compressType);
    private static final ColumnFileLoader columnFileLoader = new ColumnFileLoader(new File(tablePath));

    private final CStoreColumnReader.Builder supplierkeyColumnReader = readerFactory.openLongZipReader(columnFileLoader.open("l_supplierkey.tar"), BigintType.BIGINT,
            rowCount, pageSize, decompressor);
    private final CStoreColumnReader.Builder extendedpriceColumnReader = readerFactory.openDoubleZipReader(columnFileLoader.open("l_extendedprice.tar"), DoubleType.DOUBLE,
            rowCount, pageSize, decompressor);
    private final CStoreColumnReader.Builder taxColumnReader = readerFactory.openDoubleZipReader(columnFileLoader.open("l_tax.tar"), DoubleType.DOUBLE,
            rowCount, pageSize, decompressor);
    private final CStoreColumnReader.Builder discountColumnReader = readerFactory.openDoubleZipReader(columnFileLoader.open("l_discount.tar"), DoubleType.DOUBLE,
            rowCount, pageSize, decompressor);
    private final CStoreColumnReader.Builder quantityColumnReader = readerFactory.openDoubleZipReader(columnFileLoader.open("l_quantity.tar"), DoubleType.DOUBLE,
            rowCount, pageSize, decompressor);

    private final BitmapColumnReader.Builder index = readerFactory.openBitmapReader(columnFileLoader.open("l_returnflag.bitmap"));
    private final StringEncodedColumnReader.Builder returnflagColumnReader = readerFactory.openStringReader(rowCount, pageSize, decompressor, columnFileLoader.open("l_returnflag.tar"), VarcharType.VARCHAR);
    private final StringEncodedColumnReader.Builder statusColumnReader = readerFactory.openStringReader(rowCount, pageSize, decompressor, columnFileLoader.open("l_status.tar"), VarcharType.VARCHAR);

    @Test
    @Benchmark
    public void testVectorizeProject()
    {
        StringEncodedColumnReader flagColumnReader = this.returnflagColumnReader.build();
        StringDictionary dictionary = flagColumnReader.getDictionary();
        int id = dictionary.lookupId("A");
        Bitmap index = this.index.build().readObject(id);
        StringEncodedColumnReader statusColumnReader = this.statusColumnReader.build();
        //linestatus, returnflag, supplierkey, quantity, extendedprice, discount, tax
        List<CStoreColumnReader> columnReaders = ImmutableList.of(statusColumnReader, flagColumnReader, supplierkeyColumnReader.build(),
                quantityColumnReader.build(), extendedpriceColumnReader.build(), discountColumnReader.build(), taxColumnReader.build());

        columnReaders.forEach(CStoreColumnReader::setup);

        VectorCursor constVector1 = new ConstantDoubleCursor(1.0, vectorSize);
        List<VectorCursor> cursors = ImmutableList.of(
                new StringEncodedCursor(new int[vectorSize], statusColumnReader.getDictionaryValue()), //channel-0 = linestatus
                new StringEncodedCursor(new int[vectorSize], flagColumnReader.getDictionaryValue()), //channel-1 = returnflag
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
                columnReader.read(positions, 0, count, cursor, 0);
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
        int id = dictionary.lookupId("A");
        Bitmap index = this.index.build().readObject(id);
        StringEncodedColumnReader statusColumnReader = this.statusColumnReader.build();
        //extendedprice, discount, tax, linestatus, returnflag, supplierkey, quantity
        List<CStoreColumnReader> columnReaders = ImmutableList.of(extendedpriceColumnReader.build(), discountColumnReader.build(), taxColumnReader.build(),
                statusColumnReader, flagColumnReader, supplierkeyColumnReader.build(), quantityColumnReader.build());

        columnReaders.forEach(CStoreColumnReader::setup);

        List<VectorCursor> cursors = columnReaders.stream().map(reader -> reader.createVectorCursor(vectorSize)).collect(Collectors.toList());

        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];

        long readTimeNanos = 0;
        long projectTimeNanos = 0;
        while (iterator.hasNext()) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            int count = iterator.next(positions);
            for (int i = 0; i < columnReaders.size(); i++) {
                VectorCursor cursor = cursors.get(i);
                CStoreColumnReader columnReader = columnReaders.get(i);
                columnReader.read(positions, 0, count, cursor, 0);
            }
            Block[] inputBlocks = cursors.stream().map(cursor -> cursor.toBlock(count)).toArray(Block[]::new);
            Page inputPage = new Page(count, inputBlocks);
            readTimeNanos += stopwatch.elapsed(TimeUnit.NANOSECONDS);

            stopwatch = Stopwatch.createStarted();
            ImmutableList<BlockBuilder> projectOutBuilders = ImmutableList.<BlockBuilder>builder()
                    .add(DoubleType.DOUBLE.createBlockBuilder(null, count))
                    .add(DoubleType.DOUBLE.createBlockBuilder(null, count)).build();

            ImmutableList<BlockBuilder> hashOutBuilders = ImmutableList.<BlockBuilder>builder()
                    .add(BigintType.BIGINT.createBlockBuilder(null, count)).build();

            SelectedPositions selection = SelectedPositions.positionsRange(0, count);
            Work<List<Block>> projectWork = new PageProjectionWorkPresto(projectOutBuilders, null, inputPage, selection);
            boolean done = projectWork.process();
            //Work<List<Block>> hashWork = new PageJavaHashWork(hashOutBuilders, null, inputPage, selection);
            Work<List<Block>> hashWork = new PagePrestoHashWork(hashOutBuilders, null, inputPage, selection);
            done = done && hashWork.process();
            if (done) {
                List<Block> projectWorkResult = projectWork.getResult();
                List<Block> hashWorkResult = hashWork.getResult();
                Block[] outBlocks = ImmutableList.<Block>builder()
                        .add(inputBlocks)
                        .addAll(projectWorkResult)
                        .addAll(hashWorkResult)
                        .build().toArray(new Block[0]);
                Page outPage = new Page(count, outBlocks);
            }
            else {
                throw new IllegalStateException();
            }
            projectTimeNanos += stopwatch.elapsed(TimeUnit.NANOSECONDS);
        }
        //columnReaders.forEach(CStoreColumnReader::close);
        log.info("read cost %d ms, project cost %d ms", TimeUnit.NANOSECONDS.toMillis(readTimeNanos),
                TimeUnit.NANOSECONDS.toMillis(projectTimeNanos));
    }

    public static void main(String[] args)
            throws RunnerException, IOException
    {
        Options options = new OptionsBuilder()
                .warmupMode(WarmupMode.INDI)
                .include(VectorizeProjectionBenchmark.class.getCanonicalName() + "\\.testCodeGenerateProject")
                .build();

        new Runner(options).run();
    }
}
