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
    //private final int[] keySizeArray;
    //private final int[] aggSizeArray;
    private final int[] hashVector;
    private final int[] bucketOffsets;

    private final int[] keyOffsets;
    private final int[] aggStateOffsets;

    public PartialAggregator(
            int keySize, int aggStateSize,
            List<AggregationCall> aggCalls,
            BufferComparator keyComparator,
            File tmpDirectory,
            ExecutorManager executorManager,
            int[] keySizeArray,
            int[] aggSizeArray,
            int vectorSize)
    {
        super(keySize, aggStateSize, 10, 20, keyComparator, tmpDirectory, executorManager);
        this.aggregationCalls = aggCalls;
        //this.aggSizeArray = aggSizeArray;

        this.keyBuffer = ByteBuffer.allocate(keySize * vectorSize);
        //this.keySizeArray = keySizeArray;
        this.hashVector = new int[vectorSize];
        this.bucketOffsets = new int[vectorSize];

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

    public void addBatch(List<AggregationCursor> key, List<AggregationCursor> agg, int[] positions, int rowCount)
    {
        keyBuffer.clear();
        for (int i = 0; i < key.size(); i++) {
            AggregationCursor cursor = key.get(i);
            cursor.appendTo(keyBuffer, keyOffsets[i], keySize, positions, rowCount);
        }
        keyBuffer.position(0);
        for (int i = 0; i < rowCount; i++) {
            keyBuffer.limit(keyBuffer.position() + keySize);
            hashVector[i] = BufferUtil.hash(keyBuffer);
            keyBuffer.position(keyBuffer.limit());
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
                AggregationCursor cursor = agg.get(j);
                for (int i = totalPutCount; i < rowCount; i++) {
                    int bucketOffset = bucketOffsets[i];
                    if (bucketOffset < 0) {
                        bucketOffset = -bucketOffset;
                        int offset = aggStateOffsets[j] + getBucketValueOffset(bucketOffset);
                        aggregationCall.init(buffer, offset);
                    }
                    int offset = aggStateOffsets[j] + getBucketValueOffset(bucketOffset);
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
            cursor.appendTo(keyBuffer, keyOffsets[i], keySize, rowOffset, rowCount);
        }
        keyBuffer.position(0);
        for (int i = 0; i < rowCount; i++) {
            keyBuffer.limit(keyBuffer.position() + keySize);
            hashVector[i] = BufferUtil.hash(keyBuffer);
            keyBuffer.position(keyBuffer.limit());
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
                AggregationCursor cursor = agg.get(j);
                for (int i = totalPutCount; i < rowCount; i++) {
                    int bucketOffset = bucketOffsets[i];
                    if (bucketOffset < 0) {
                        bucketOffset = -bucketOffset;
                        int offset = aggStateOffsets[j] + getBucketValueOffset(bucketOffset);
                        aggregationCall.init(buffer, offset);
                    }
                    int offset = aggStateOffsets[j] + getBucketValueOffset(bucketOffset);
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
