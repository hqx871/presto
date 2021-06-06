package org.apache.cstore.aggregation;

import org.apache.cstore.util.BufferUtil;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * array: bucket index, with bucket offset in the buffer.
 * buffer: magic header two bytes and repeated buckets.
 * bucket: hash code, next bucket element offset, key, value.
 * magic_header_size = 2.
 * bucket_size = 4 + 4 + key_size + value_size.
 * bucket_offset = bucket_id * bucket_size + magic_header_size
 */
public abstract class LinkedHashTable
{
    protected final int keySize;
    protected final int valueSize;
    protected ByteBuffer buffer;
    protected int capacity;
    protected final int bucketSize;
    protected int count;
    protected int[] buckets;
    protected final int magic = 2;
    private final float factor = 0.75F;
    protected int threshold;
    private final int maxCapacity;
    protected final int bucketHeaderSize;
    private int bucketMask;
    private final MemoryManager memoryManager;

    public LinkedHashTable(int keySize,
            int valueSize,
            int capacityBit,
            int maxCapacityBit,
            MemoryManager memoryManager)
    {
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.capacity = 1 << capacityBit;
        this.maxCapacity = 1 << maxCapacityBit;
        this.bucketHeaderSize = 8;
        this.bucketSize = bucketHeaderSize + keySize + valueSize;
        this.count = 0;
        this.buckets = new int[capacity];
        this.threshold = (int) (capacity * factor);
        this.bucketMask = capacity - 1;
        this.memoryManager = memoryManager;
        this.buffer = memoryManager.allocate(getBucketOffset(threshold));

        buffer.putChar('H');
    }

    protected final int findBucketAndPut(ByteBuffer keyBuffer, int offset, int size, int hash)
    {
        keyBuffer.position(offset);
        keyBuffer = keyBuffer.slice();
        keyBuffer.limit(size);
        return findBucketAndPut(keyBuffer, hash);
    }

    /**
     * @return bucket offset
     * negative means new bucket,
     * positive the key has been added,
     * zero means fail
     */
    protected final int findBucketAndPut(ByteBuffer key, int hash)
    {
        if (size() >= threshold && !resize()) {
            return 0;
        }

        int bucketId = bucketId(bucketMask, hash);
        int bucketOffset = buckets[bucketId];
        while (bucketOffset > 0) {
            int bucketHashCode = buffer.getInt(bucketOffset);
            if (bucketHashCode == hash && BufferUtil.equals(buffer, bucketOffset + bucketHeaderSize, key, key.position(), keySize)) {
                return bucketOffset;
            }
            bucketOffset = buffer.getInt(bucketOffset + 4);
        }

        buckets[bucketId] = putToBucket(key, hash, buckets[bucketId]);
        return -buckets[bucketId];
    }

    private int putToBucket(ByteBuffer key, int hash, int lastBucket)
    {
        int offset = getBucketOffset(count++);
        buffer.position(offset);
        buffer.putInt(hash);
        buffer.putInt(lastBucket);
        buffer.put(key);
        return offset;
    }

    private static int bucketId(int mask, int hash)
    {
        return hash & mask;
    }

    public int size()
    {
        return count;
    }

    protected final int getBucketOffset(int rowNo)
    {
        return magic + bucketSize * rowNo;
    }

    private boolean resize()
    {
        if (capacity >= maxCapacity) {
            return false;
        }

        int[] newBuckets = new int[buckets.length * 2];
        int newThread = (int) (newBuckets.length * factor);
        int newCapacity = capacity * 2;
        int newMask = newCapacity - 1;
        ByteBuffer newBuffer = memoryManager.allocate(getBucketOffset(newThread));
        newBuffer.put(BufferUtil.slice(buffer, 0, getBucketOffset(count)));

        for (int i = 0; i < count; i++) {
            int bucketOffset = getBucketOffset(i);
            int hash = newBuffer.getInt(bucketOffset);
            int newBucketId = bucketId(newMask, hash);
            newBuffer.putInt(bucketOffset + 4, newBuckets[newBucketId]);
            newBuckets[newBucketId] = bucketOffset;
        }

        this.buckets = newBuckets;
        this.buffer = newBuffer;
        this.threshold = newThread;
        this.capacity = newCapacity;
        this.bucketMask = newMask;
        return true;
    }

    protected final void clear()
    {
        this.count = 0;
        Arrays.fill(buckets, 0);
    }

    public int getKeySize()
    {
        return keySize;
    }

    public int getValueSize()
    {
        return valueSize;
    }

    protected final int getBucketValueOffset(int bucketOffset)
    {
        return bucketHeaderSize + bucketOffset;
    }
}
