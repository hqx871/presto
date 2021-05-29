package org.apache.cstore.util;

import com.google.common.base.Stopwatch;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public enum PerfLog
{
    DEFAULT("total", true),

    FILTER_ANALYZE_BITMAP("FilterAnalyze.bitmap", true),

    @Deprecated
    CURSOR_FIRST_READ("Cursor.firstRead", false),
    @Deprecated
    CURSOR_RESULT_READ("Cursor.resultRead", true),

    LINKED_HASH_TABLE_FIND_BUCKET_AND_PUT("LinkedHashTable.findAndPutToBucket", true),
    LINKED_HASH_TABLE_RESIZE("LinkedHashTable.resize", true),

    BUFFER_UTIL_HASH("BufferUtil.hash", true),
    BUFFER_UTIL_EQUAL("BufferUtil.equals", true),

    BUFFER_UTIL_SORT("BufferUtil.sort", true),
    BUFFER_UTIL_SORT_MERGE("BufferUtil.sortMerge", true),

    BUFFER_UTIL_COMPARE("BufferUtil.compare", true),
    BUFFER_UTIL_SWAP("BufferUtil.swap", true),
    BUFFER_UTIL_SLICE("BufferUtil.slice", true),

    GROUP_HASH_EQUAL("BufferHashTable.equal", true),
    GROUP_HASH_EQUAL_KEY("BufferHashTable.equalKey", true),

    SCAN_FILTER_NEXT("ScanFilter.next", true),

    VECTOR_BINARY_FIX_READ_INT("BinaryFixLenVector.readInt", false),
    VECTOR_BINARY_FIX_READ_LONG("BinaryFixLenVector.readLong", false),
    VECTOR_BINARY_FIX_READ_DOUBLE("BinaryFixLenVector.readLong", false),
    VECTOR_BINARY_FIX_READ_STRING("BinaryFixLenVector.readString", false),
    VECTOR_STRING_ENCODED_READ("StringEncodedVector.read", true),
    VECTOR_STRING_ENCODED_BYTE_READ("StringEncodedByteVector.read", true),
    VECTOR_STRING_ENCODED_BATCH_READ_VALUE("StringEncodedVector.batchReadValue", false),
    VECTOR_STRING_ENCODED_BATCH_READ_ID("StringEncodedVector.batchReadId", true),

    VECTOR_BYTE_BUFFER_READ_RANGE("ByteBufferVector.readRange", true),

    VECTOR_STRING_ENCODED_SHORT_READ("StringEncodedShortVector.read", true),

    VECTOR_INT_BUFFER_BATCH_READ("IntBufferVector.batchRead", true),

    DICT_TRIE_BUFFER_DECODE_CACHE("TrieBufferTree.valueCache", true),
    DICT_TRIE_BUFFER_DECODE("TrieBufferTree.valueNoCache", true),

    DICT_TRIE_ARRAY_CACHE_GET("DictArrayCache.get", true),

    LZ4_CODER_ENCODE("LZ4Coder.encode", true),
    LZ4_CODER_DECODE("LZ4Coder.decode", true),

    JOIN_BUILD("VectorLookupJoinOperator.build", true),
    JOIN_PROBE("VectorLookupJoinOperator.probe", true),

    ROW_COMPARATOR("RowComparator.compare", true),
    JSON_SINK("JsonSinkOperator.next", true),

    STORE_OPEN("TableStore.open", true),

    PARTIAL_AGG_HASH_WRITE_TO_KEYSPACE("PartialAggHashTable.writeToKeySpace", true),
    PARTIAL_AGG_HASH_NEXT_ROW("PartialAggHashTable.nextRow", true),
    PARTIAL_AGG_HASH_INIT_STATE("PartialAggHashTable.initState", true),
    PARTIAL_AGG_HASH_COMBINE_STATE("PartialAggHashTable.combineState", true),
    PARTIAL_AGG_HASH_CONVERT_ROW("PartialAggHashTable.convertToRow", true),
    PARTIAL_AGG_HASH_GROUP("PartialAggHashTable.group", true),
    PARTIAL_AGG_HASH_FETCH_KEY("PartialAggHashTable.fetchKey", true),
    PARTIAL_AGG_HASH_PUT_BUCKET_LIST("PartialAggHashTable.putToBucketList", true),

    PARTIAL_AGG_HASH_AGG("PartialAggHashTable.agg", true),

    HASH_TABLE_SPILL("SpillHashTable.spill", true),
    HASH_TABLE_SPILL_NEXT("SpillHashTable.next", true),
    HASH_TABLE_SPILL_HEAP_COMPARE("SpillHashTable.heapCompare", true),
    HASH_TABLE_PUT_BUCKET("LinkedHashTable.putToBucket", true),

    AGG_REDUCER_COMBINE("AggTupleReducer.combine", true),
    AGG_REDUCER_REDUCE("AggTupleReducer.reduce", true),

    SORT_MERGE_AGG_OPERATOR("SortMergeAggOperator.next", true);

    private final String id;

    private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
    private final AtomicLong cost;

    private final ThreadLocal<Stopwatch> start;

    private final AtomicLong count;

    private final boolean enable;

    PerfLog(String id, boolean enable)
    {
        this.id = id;
        this.enable = enable;
        this.start = new ThreadLocal<>();
        this.count = new AtomicLong(0);
        this.cost = new AtomicLong(0);
    }

    public PerfLog start()
    {
        if (enable) {
            count.incrementAndGet();
            start.set(Stopwatch.createStarted());
        }
        return this;
    }

    public void sum()
    {
        if (enable) {
            long cost = start.get().elapsed(TimeUnit.NANOSECONDS);
            while (!max.compareAndSet(max.get(), Math.max(max.get(), cost))) {
                //
            }
            while (!min.compareAndSet(min.get(), Math.min(min.get(), cost))) {
                //
            }
            this.cost.addAndGet(cost);
        }
    }

    public long max()
    {
        return max.get();
    }

    public void clear()
    {
        cost.set(0);
        count.set(0);
    }

    @Override
    public String toString()
    {
        return "TimeCounter{" +
                "id='" + id + '\'' +
                ", count=" + count +
                ", min=" + min +
                ", max=" + max +
                ", costUs=" + cost.get() / 1000 +
                (count.get() > 0 ? ", avgNs=" + cost.get() / count.get() : "") +
                '}';
    }

    public static Collection<PerfLog> timers()
    {
        return Arrays.stream(values())
                .filter(item -> item.enable && item.count.get() > 0)
                .collect(Collectors.toList());
    }

    public static void clearAll()
    {
        for (PerfLog item : values()) {
            item.clear();
        }
    }
}
