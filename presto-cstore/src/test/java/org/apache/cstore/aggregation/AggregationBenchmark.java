package org.apache.cstore.aggregation;

import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.VarcharType;
import com.google.common.collect.ImmutableList;
import io.airlift.compress.Decompressor;
import org.apache.cstore.BufferComparator;
import org.apache.cstore.bitmap.Bitmap;
import org.apache.cstore.bitmap.BitmapIterator;
import org.apache.cstore.coder.CompressFactory;
import org.apache.cstore.column.BitmapColumnReader;
import org.apache.cstore.column.CStoreColumnLoader;
import org.apache.cstore.column.CStoreColumnReader;
import org.apache.cstore.column.StringEncodedColumnReader;
import org.apache.cstore.column.VectorCursor;
import org.apache.cstore.dictionary.StringDictionary;
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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
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
    private static final String compressType = TpchTableGenerator.compressType;
    private static final int vectorSize = 1024;
    private static final int rowCount = 6001215;
    private static final int pageSize = TpchTableGenerator.pageSize;
    private final Decompressor decompressor = CompressFactory.INSTANCE.getDecompressor(compressType);

    private final CStoreColumnReader.Builder extendedpriceColumnReader = readerFactory.openDoubleZipReader(tablePath, "l_extendedprice", DoubleType.DOUBLE,
            rowCount, pageSize, decompressor);
    private final CStoreColumnReader.Builder taxColumnReader = readerFactory.openDoubleZipReader(tablePath, "l_tax", DoubleType.DOUBLE,
            rowCount, pageSize, decompressor);
    private final CStoreColumnReader.Builder discountColumnReader = readerFactory.openDoubleZipReader(tablePath, "l_discount", DoubleType.DOUBLE,
            rowCount, pageSize, decompressor);
    private final BitmapColumnReader.Builder index = readerFactory.openBitmapReader(tablePath, "l_returnflag");
    private final StringEncodedColumnReader.Builder returnflagColumnReader = readerFactory.openStringReader(rowCount, TpchTableGenerator.pageSize, decompressor, tablePath, "l_returnflag", VarcharType.VARCHAR);
    private final StringEncodedColumnReader.Builder statusColumnReader = readerFactory.openStringReader(rowCount, TpchTableGenerator.pageSize, decompressor, tablePath, "l_status", VarcharType.VARCHAR);

    @Test
    @Benchmark
    public void testHashMergeAggregator()
    {
        StringEncodedColumnReader flagColumnReader = this.returnflagColumnReader.duplicate();
        StringDictionary dictionary = flagColumnReader.getDictionary();
        int id = dictionary.encodeId("A");
        Bitmap index = this.index.duplicate().readObject(id);
        StringEncodedColumnReader statusColumnReader = this.statusColumnReader.duplicate();
        List<CStoreColumnReader> columnReaders = ImmutableList.of(statusColumnReader,
                taxColumnReader.duplicate(), extendedpriceColumnReader.duplicate());

        List<AggregationCursor> keyCursors = ImmutableList.of(new AggregationStringCursor(new int[vectorSize],
                statusColumnReader.getDictionaryValue()));
        List<AggregationCursor> aggCursors = ImmutableList.of(new AggregationDoubleCursor(new long[vectorSize]),
                new AggregationDoubleCursor(new long[vectorSize]));
        int[] keySizeArray = new int[] {4};
        int keySize = IntStream.of(keySizeArray).sum();
        List<AggregationCall> aggregationCalls = ImmutableList.of(new DoubleSumCall(), new DoubleAvgCall());
        int[] aggSizeArray = aggregationCalls.stream().mapToInt(AggregationCall::getStateSize).toArray();
        BufferComparator keyComparator = new BufferComparator()
        {
            @Override
            public int compare(ByteBuffer a, int oa, ByteBuffer b, int ob)
            {
                return a.getInt(oa) - b.getInt(ob);
            }
        };
        PartialAggregator partialAggregator = new PartialAggregator(aggregationCalls, keyComparator,
                new File("presto-cstore/target"), new ExecutorManager(),
                keySizeArray, aggSizeArray, vectorSize);

        partialAggregator.setup();

        List<AggregationCursor> cursors = ImmutableList.<AggregationCursor>builder().addAll(keyCursors).addAll(aggCursors).build();

        BitmapIterator iterator = index.iterator();
        int[] positions = new int[vectorSize];

        while (iterator.hasNext()) {
            int count = iterator.next(positions);
            for (int i = 0; i < cursors.size(); i++) {
                VectorCursor cursor = cursors.get(i);
                CStoreColumnReader columnReader = columnReaders.get(i);
                columnReader.read(positions, 0, count, cursor);
            }
            partialAggregator.addBatch(keyCursors, aggCursors, 0, count);
        }
        AggregationReducer reducer = new AggregationReducerImpl(keySize, aggregationCalls);
        SortMergeAggregator mergeAggregator = new SortMergeAggregator(
                ImmutableList.of(partialAggregator.rawIterator()),
                reducer,
                false);
        mergeAggregator.setup();
        Iterator<ByteBuffer> result = mergeAggregator.iterator();
        while (result.hasNext()) {
            result.next();
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
