package github.cstore.aggregation;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.cstore.storage.CStoreColumnLoader;
import com.google.common.collect.ImmutableList;
import github.cstore.aggregation.call.CountStarCall;
import github.cstore.aggregation.call.DoubleAvgCall;
import github.cstore.aggregation.call.DoubleSumCall;
import github.cstore.aggregation.cursor.AggregationDoubleCursor;
import github.cstore.aggregation.cursor.AggregationLongCursor;
import github.cstore.aggregation.cursor.AggregationStringCursor;
import github.cstore.bitmap.Bitmap;
import github.cstore.bitmap.BitmapIterator;
import github.cstore.coder.CompressFactory;
import github.cstore.column.BitmapColumnReader;
import github.cstore.column.CStoreColumnReader;
import github.cstore.column.ConstantDoubleCursor;
import github.cstore.column.DoubleCursor;
import github.cstore.column.LongCursor;
import github.cstore.column.StringCursor;
import github.cstore.column.StringEncodedColumnReader;
import github.cstore.column.VectorCursor;
import github.cstore.dictionary.StringDictionary;
import github.cstore.filter.SelectedPositions;
import github.cstore.projection.DoubleMinusCall;
import github.cstore.projection.DoubleMultipleCall;
import github.cstore.projection.DoublePlusCall;
import github.cstore.projection.ScalarCall;
import github.cstore.sort.BufferComparator;
import github.cstore.util.ExecutorManager;
import github.cstore.util.MemoryManager;
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
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 2)
@Warmup(iterations = 15)
@Measurement(iterations = 15)
public class AggregationBenchmark
{
    private static final String tablePath = "presto-cstore/sample-data/tpch/lineitem";
    private static final CStoreColumnLoader readerFactory = new CStoreColumnLoader();
    private static final String compressType = "lz4";
    private static final int vectorSize = 1024;
    private static final int rowCount = 6001215;
    private static final int pageSize = 64 << 10;
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
    private final StringEncodedColumnReader.Builder returnflagColumnReader = readerFactory.openStringReader(rowCount, 64 << 10, decompressor, tablePath, "l_returnflag", VarcharType.VARCHAR);
    private final StringEncodedColumnReader.Builder statusColumnReader = readerFactory.openStringReader(rowCount, 64 << 10, decompressor, tablePath, "l_status", VarcharType.VARCHAR);

    @Test
    @Benchmark
    public void testHashMergeAggregator()
    {
        StringEncodedColumnReader flagColumnReader = this.returnflagColumnReader.build();
        StringDictionary dictionary = flagColumnReader.getDictionary();
        int id = dictionary.encodeId("A");
        Bitmap index = this.index.build().readObject(id);
        StringEncodedColumnReader statusColumnReader = this.statusColumnReader.build();
        //linestatus, returnflag, supplierkey, quantity, extendedprice, discount, tax
        List<CStoreColumnReader> columnReaders = ImmutableList.of(statusColumnReader, flagColumnReader, supplierkeyColumnReader.build(),
                quantityColumnReader.build(), extendedpriceColumnReader.build(), discountColumnReader.build(), taxColumnReader.build());

        AggregationDoubleCursor constVector1 = new AggregationDoubleCursor(new ConstantDoubleCursor(1.0, vectorSize));
        List<AggregationCursor> cursorWrappers = ImmutableList.of(
                new AggregationStringCursor(new StringCursor(new int[vectorSize], statusColumnReader.getDictionaryValue())), //channel-0 = linestatus
                new AggregationStringCursor(new StringCursor(new int[vectorSize], flagColumnReader.getDictionaryValue())), //channel-1 = returnflag
                new AggregationLongCursor(new LongCursor(new long[vectorSize])), //channel-2 = supplierkey
                new AggregationDoubleCursor(new DoubleCursor(new long[vectorSize])), //channel-3 = quantity
                new AggregationDoubleCursor(new DoubleCursor(new long[vectorSize])), //channel-4 = extendedprice
                new AggregationDoubleCursor(new DoubleCursor(new long[vectorSize])), //channel-5 = discount
                new AggregationDoubleCursor(new DoubleCursor(new long[vectorSize])), //channel-6 = tax
                constVector1, //channel-7 = constant 1.0
                new AggregationDoubleCursor(new DoubleCursor(new long[vectorSize])), //channel-8 =  1 - discount
                new AggregationDoubleCursor(new DoubleCursor(new long[vectorSize])), //channel-9 =  (1 - discount) * extendedprice
                new AggregationDoubleCursor(new DoubleCursor(new long[vectorSize])), //channel-10 =  (1 + tax)
                new AggregationDoubleCursor(new DoubleCursor(new long[vectorSize]))); //channel-11 =  extendedprice * (1 - discount) * (1 + tax)

        List<VectorCursor> cursors = cursorWrappers.stream().map(AggregationCursor::getVectorCursor).collect(Collectors.toList());

        List<ScalarCall> projectionCalls = ImmutableList.of(new DoubleMinusCall(7, 5, 8),
                new DoubleMultipleCall(8, 4, 9),
                new DoublePlusCall(7, 6, 10),
                new DoubleMultipleCall(9, 10, 11));

        int[] keyCursorOrdinals = new int[] {0, 1, 2};
        List<AggregationCursor> keyCursors = IntStream.of(keyCursorOrdinals).mapToObj(cursorWrappers::get).collect(Collectors.toList());

        List<AggregationCall> aggregationCalls = ImmutableList.of(new DoubleSumCall(3), //sum(l_quantity)
                new DoubleSumCall(4), //sum(l_extendedprice)
                new DoubleSumCall(9), //sum(l_extendedprice * (1 - l_discount))
                new DoubleSumCall(11), //sum(l_extendedprice * (1 - l_discount) * (1 + l_tax))
                new DoubleAvgCall(3), //avg(l_quantity)
                new DoubleAvgCall(4), //avg(l_extendedprice)
                new DoubleAvgCall(5), //avg(l_discount)
                new CountStarCall());

        int[] keySizeArray = keyCursors.stream().mapToInt(AggregationCursor::getKeySize).toArray();
        int keySize = IntStream.of(keySizeArray).sum();
        int[] aggSizeArray = aggregationCalls.stream().mapToInt(AggregationCall::getStateSize).toArray();
        BufferComparator keyComparator = new KeyComparator(keyCursors, keySizeArray);

        MemoryManager memoryManager = new MemoryManager();
        PartialAggregator partialAggregator = new PartialAggregator(aggregationCalls, keyComparator,
                new File("presto-cstore/target"), new ExecutorManager("aggregation-%d"), memoryManager,
                keyCursorOrdinals, keySizeArray, aggSizeArray, vectorSize);

        partialAggregator.setup();

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
            partialAggregator.addPage(cursorWrappers, SelectedPositions.positionsRange(0, count));
        }
        AggregationReducer reducer = new AggregationReducerImpl(keySize, aggregationCalls);
        SortMergeAggregator mergeAggregator = new SortMergeAggregator(
                ImmutableList.of(partialAggregator.getResult()),
                reducer,
                false);
        mergeAggregator.setup();
        Iterator<ByteBuffer> result = mergeAggregator.iterator();
        List<VectorCursor> outCursors = ImmutableList.of(new StringCursor(new int[vectorSize], statusColumnReader.getDictionaryValue()),
                new StringCursor(new int[vectorSize], flagColumnReader.getDictionaryValue()),
                new LongCursor(new long[vectorSize]),
                new DoubleCursor(new long[vectorSize]),
                new DoubleCursor(new long[vectorSize]),
                new DoubleCursor(new long[vectorSize]),
                new DoubleCursor(new long[vectorSize]),
                new DoubleCursor(new long[vectorSize]),
                new DoubleCursor(new long[vectorSize]),
                new DoubleCursor(new long[vectorSize]),
                new LongCursor(new long[vectorSize]));

        while (result.hasNext()) {
            int resultCount = 0;
            int count = 0;
            while (count < vectorSize && result.hasNext()) {
                ByteBuffer rawRow = result.next();
                for (int j = 0; j < 2; j++) {
                    outCursors.get(j).writeInt(count, rawRow.getInt());
                }
                outCursors.get(2).writeLong(count, rawRow.getLong());
                for (int j = 3; j < 10; j++) {
                    outCursors.get(j).writeDouble(count, rawRow.getDouble());
                }
                outCursors.get(10).writeLong(count, rawRow.getLong());
                count++;
            }
            Block[] blocks = new Block[cursors.size()];
            for (int i = 0; i < outCursors.size(); i++) {
                blocks[i] = cursors.get(i).toBlock(resultCount);
            }
            Page page = new Page(resultCount, blocks);
        }
        partialAggregator.close();
        mergeAggregator.close();
    }

    public static void main(String[] args)
            throws RunnerException, IOException
    {
        Options options = new OptionsBuilder()
                .warmupMode(WarmupMode.INDI)
                .include(AggregationBenchmark.class.getCanonicalName() + "\\.test.*")
                .build();

        new Runner(options).run();
    }
}
