package org.apache.cstore.aggregation;

import org.apache.cstore.BufferComparator;
import org.apache.cstore.util.BufferUtil;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

public class PartialAggregator
        extends SpillHashTable
{
    private final List<AggregationCall> aggregationCalls;
    private final ByteBuffer keyBuffer;
    private final int[] keyCursorSize;
    private final int[] aggregationCursorSize;
    private final int[] hashBuffer;
    private final int[] bucketOffsets;

    public PartialAggregator(
            int keySize, int aggregationStateSize,
            List<AggregationCall> aggregationCalls,
            BufferComparator keyComparator,
            File tmpDirectory,
            ExecutorManager executorManager,
            int[] keyCursorSize,
            int[] aggregationCursorSize,
            int vectorSize)
    {
        super(keySize, aggregationStateSize, 10, 20, keyComparator, tmpDirectory, executorManager);
        this.aggregationCalls = aggregationCalls;
        this.aggregationCursorSize = aggregationCursorSize;

        this.keyBuffer = ByteBuffer.allocate(keySize * vectorSize);
        this.keyCursorSize = keyCursorSize;
        this.hashBuffer = new int[vectorSize];
        this.bucketOffsets = new int[vectorSize];
    }

    public void setup()
    {
    }

    public void addBatch(List<AggregationCursor> key, List<AggregationCursor> agg, int[] positions, int rowCount)
    {
        keyBuffer.clear();
        for (int i = 0; i < key.size(); i++) {
            AggregationCursor cursor = key.get(i);
            cursor.appendTo(keyBuffer, keyCursorSize[i], keySize, positions, rowCount);
        }
        keyBuffer.position(0);
        for (int i = 0; i < rowCount; i++) {
            keyBuffer.limit(i * keySize);
            hashBuffer[i] = BufferUtil.hash(keyBuffer);
        }
        keyBuffer.position(0);
        int totalPutCount = 0;
        while (totalPutCount < rowCount) {
            int curPutCount = 0;
            for (int i = totalPutCount; i < rowCount; i++) {
                keyBuffer.limit(i * keySize);
                bucketOffsets[i] = findBucketAndPut(keyBuffer, hashBuffer[i]);
                curPutCount++;
                if (bucketOffsets[i] == 0) {
                    break;
                }
            }

            for (int j = 0; j < aggregationCalls.size(); j++) {
                int offset = keySize + aggregationCursorSize[j];
                AggregationCall aggregationCall = aggregationCalls.get(j);
                AggregationCursor cursor = agg.get(j);
                for (int i = totalPutCount; i < rowCount; i++) {
                    int bucketOffset = bucketOffsets[i];
                    if (bucketOffset < 0) {
                        bucketOffset = -bucketOffset;
                        offset += bucketOffset;
                        aggregationCall.init(buffer, offset);
                    }
                    aggregationCall.add(buffer, offset, cursor, positions[i]);
                }
            }
            totalPutCount += curPutCount;
            if (totalPutCount < rowCount) {
                doSpill();
            }
        }
    }

    public void addBatch(List<AggregationCursor> key, List<AggregationCursor> agg, int rowOffset, int rowCount)
    {
        keyBuffer.clear();
        for (int i = 0; i < key.size(); i++) {
            AggregationCursor cursor = key.get(i);
            cursor.appendTo(keyBuffer, keyCursorSize[i], keySize, rowOffset, rowCount);
        }
        keyBuffer.position(0);
        for (int i = 0; i < rowCount; i++) {
            keyBuffer.limit(i * keySize);
            hashBuffer[i] = BufferUtil.hash(keyBuffer);
        }
        keyBuffer.position(0);
        int totalPutCount = 0;
        while (totalPutCount < rowCount) {
            int curPutCount = 0;
            for (int i = totalPutCount; i < rowCount; i++) {
                keyBuffer.limit(i * keySize);
                bucketOffsets[i] = findBucketAndPut(keyBuffer, hashBuffer[i]);
                curPutCount++;
                if (bucketOffsets[i] == 0) {
                    break;
                }
            }

            for (int j = 0; j < aggregationCalls.size(); j++) {
                int offset = keySize + aggregationCursorSize[j];
                AggregationCall aggregationCall = aggregationCalls.get(j);
                AggregationCursor cursor = agg.get(j);
                for (int i = totalPutCount; i < rowCount; i++) {
                    int bucketOffset = bucketOffsets[i];
                    if (bucketOffset < 0) {
                        bucketOffset = -bucketOffset;
                        offset += bucketOffset;
                        aggregationCall.init(buffer, offset);
                    }
                    aggregationCall.add(buffer, offset, cursor, i + rowOffset);
                }
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
