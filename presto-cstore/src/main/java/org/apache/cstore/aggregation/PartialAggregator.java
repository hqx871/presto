package org.apache.cstore.aggregation;

import org.apache.cstore.aggregation.hash.SpillHashTable;
import org.apache.cstore.filter.SelectedPositions;
import org.apache.cstore.sort.BufferComparator;
import org.apache.cstore.util.BufferUtil;
import org.apache.cstore.util.ExecutorManager;
import org.apache.cstore.util.MemoryManager;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

public class PartialAggregator
        extends SpillHashTable
{
    private final List<AggregationCall> aggregationCalls;
    private final ByteBuffer keyBuffer;
    //private final int[] keySizeArray;
    //private final int[] aggSizeArray;
    private final int[] hashVector;
    private final int[] bucketOffsets;
    private final int[] bucketValueOffsets;

    private final int[] keyOffsets;
    private final int[] aggStateOffsets;
    private final int[] keyCursorOrdinals;

    public PartialAggregator(
            List<AggregationCall> aggCalls,
            BufferComparator keyComparator,
            File tmpDirectory,
            ExecutorManager executorManager,
            MemoryManager memoryManager,
            int[] keyCursorOrdinals,
            int[] keySizeArray,
            int[] aggSizeArray,
            int vectorSize)
    {
        super(IntStream.of(keySizeArray).sum(), IntStream.of(aggSizeArray).sum(),
                10, 24,
                keyComparator, tmpDirectory, executorManager, memoryManager);
        this.aggregationCalls = aggCalls;
        this.keyCursorOrdinals = keyCursorOrdinals;
        //this.aggSizeArray = aggSizeArray;

        this.keyBuffer = memoryManager.allocate(keySize * vectorSize);
        //this.keySizeArray = keySizeArray;
        this.hashVector = new int[vectorSize];
        this.bucketOffsets = new int[vectorSize];
        this.bucketValueOffsets = new int[vectorSize];

        this.keyOffsets = new int[keySizeArray.length];
        for (int i = 1; i < keyOffsets.length; i++) {
            keyOffsets[i] = keyOffsets[i - 1] + keySizeArray[i - 1];
        }
        this.aggStateOffsets = new int[aggSizeArray.length];
        aggStateOffsets[0] = keySize;
        for (int i = 1; i < aggStateOffsets.length; i++) {
            aggStateOffsets[i] = aggSizeArray[i - 1] + aggStateOffsets[i - 1];
        }
    }

    public void setup()
    {
    }

    public void addPage(List<AggregationCursor> page, SelectedPositions selection)
    {
        if (selection.isList()) {
            addPage(page, selection.getPositions(), selection.size());
        }
        else {
            addPage(page, selection.getOffset(), selection.size());
        }
    }

    public void addPage(List<AggregationCursor> cursors, int[] positions, int rowCount)
    {
        keyBuffer.clear();
        for (int i = 0; i < keyCursorOrdinals.length; i++) {
            AggregationCursor cursor = cursors.get(keyCursorOrdinals[i]);
            cursor.appendTo(keyBuffer, keyOffsets[i], keySize, positions, rowCount);
        }
        keyBuffer.position(0);
        for (int i = 0; i < rowCount; i++) {
            keyBuffer.limit(keyBuffer.position() + keySize);
            hashVector[i] = BufferUtil.hash(keyBuffer);
        }
        keyBuffer.position(0);
        int totalPutCount = 0;
        while (totalPutCount < rowCount) {
            int curPutCount = 0;
            for (int i = totalPutCount; i < rowCount; i++) {
                keyBuffer.limit(keyBuffer.position() + keySize);
                bucketOffsets[i] = findBucketAndPut(keyBuffer, hashVector[i]);
                keyBuffer.position(keyBuffer.limit());
                curPutCount++;
                if (bucketOffsets[i] == 0) {
                    break;
                }
            }

            for (int j = 0; j < aggregationCalls.size(); j++) {
                AggregationCall aggregationCall = aggregationCalls.get(j);
                for (int i = totalPutCount; i < rowCount; i++) {
                    int bucketOffset = bucketOffsets[i];
                    if (bucketOffset < 0) {
                        bucketOffset = -bucketOffset;
                        int offset = aggStateOffsets[j] + getBucketValueOffset(bucketOffset);
                        aggregationCall.init(buffer, offset);
                    }
                    int offset = aggStateOffsets[j] + getBucketValueOffset(bucketOffset);
                    bucketValueOffsets[i] = offset;
                }
                aggregationCall.add(buffer, bucketValueOffsets, 0, cursors, totalPutCount, curPutCount);
            }
            totalPutCount += curPutCount;
            if (totalPutCount < rowCount) {
                doSpill();
            }
        }
    }

    public void addPage(List<AggregationCursor> cursors, int rowOffset, int rowCount)
    {
        keyBuffer.clear();
        for (int i = 0; i < keyCursorOrdinals.length; i++) {
            AggregationCursor cursor = cursors.get(keyCursorOrdinals[i]);
            cursor.appendTo(keyBuffer, keyOffsets[i], keySize, rowOffset, rowCount);
        }
        keyBuffer.position(0);
        for (int i = 0; i < rowCount; i++) {
            keyBuffer.limit(keyBuffer.position() + keySize);
            hashVector[i] = BufferUtil.hash(keyBuffer);
        }
        keyBuffer.position(0);
        int totalPutCount = 0;
        while (totalPutCount < rowCount) {
            int curPutCount = 0;
            for (int i = totalPutCount; i < rowCount; i++) {
                keyBuffer.limit(keyBuffer.position() + keySize);
                bucketOffsets[i] = findBucketAndPut(keyBuffer, hashVector[i]);
                keyBuffer.position(keyBuffer.limit());
                curPutCount++;
                if (bucketOffsets[i] == 0) {
                    break;
                }
            }

            for (int j = 0; j < aggregationCalls.size(); j++) {
                AggregationCall aggregationCall = aggregationCalls.get(j);
                for (int i = totalPutCount; i < rowCount; i++) {
                    int bucketOffset = bucketOffsets[i];
                    if (bucketOffset < 0) {
                        bucketOffset = -bucketOffset;
                        int offset = aggStateOffsets[j] + getBucketValueOffset(bucketOffset);
                        aggregationCall.init(buffer, offset);
                        bucketValueOffsets[i] = offset;
                    }
                    else {
                        int offset = aggStateOffsets[j] + getBucketValueOffset(bucketOffset);
                        bucketValueOffsets[i] = offset;
                    }
                }
                aggregationCall.add(buffer, bucketValueOffsets, totalPutCount, cursors, rowOffset + totalPutCount, curPutCount);
            }
            totalPutCount += curPutCount;
            if (totalPutCount < rowCount) {
                doSpill();
            }
        }
    }

    public Iterator<ByteBuffer> getResult()
    {
        return super.rawIterator();
    }

    public void close()
    {
    }
}
