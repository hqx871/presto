package org.apache.cstore.util;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.cstore.BufferComparator;

import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Comparator;
import java.util.List;

public final class BufferUtil
{
    private BufferUtil()
    {
    }

    public static int hash(ByteBuffer key)
    {
        //int hash = xxHash32(key);
        return murmurHash32(key);
    }

    private static final int HASH_SEED = 0;

    public static int hash(ByteBuffer key, int offset, int size)
    {
        //int hash = XXHASH32.hash(slice(key, offset, size), HASH_SEED);
        return MURMUR3_HASH32.hashBytes(slice(key, offset, size)).asInt();
    }

    public static void hash(ByteBuffer keyspace, int keySize, int cnt, int[] hash)
    {
        int p = keyspace.position();
        for (int i = 0, offset = 0; i < cnt; offset += keySize, i++) {
            keyspace.position(offset);
            keyspace.limit(offset + keySize);
            hash[i] = MURMUR3_HASH32.hashBytes(keyspace).asInt();
        }
        keyspace.position(p);
    }

    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    public static int smearHash(ByteBuffer key, int offset, int size)
    {
        int hash = slice(key, offset, size).hashCode();
        hash = C2 * Integer.rotateLeft(hash * C1, 15);
        return hash;
    }

    public static int simpleHash(ByteBuffer key, int offset, int size)
    {
        int hash = 0;
        key = slice(key, offset, size);
        while (key.remaining() >= 8) {
            hash = hash * 31 + Long.hashCode(key.getLong());
        }
        if (key.remaining() >= 4) {
            hash = hash * 31 + key.getInt();
        }
        if (key.remaining() >= 2) {
            hash = hash * 31 + key.getShort();
        }
        while (key.remaining() >= 1) {
            hash = hash * 31 + key.get();
        }
        return hash;
    }

    //private static final LongHashFunction XXHASH64 = LongHashFunction.xx();
    private static final XXHash32 XXHASH32 = XXHashFactory.fastestInstance().hash32();

    public static int xxHash32(ByteBuffer key)
    {
        return XXHASH32.hash(key.asReadOnlyBuffer(), HASH_SEED);
        //long hash = XXHASH64.hashBytes(key.asReadOnlyBuffer());
        //return (int) ((hash >> 32) & hash);
    }

    public static int jdkHash(ByteBuffer key)
    {
        return key.hashCode();
    }

    private static final XXHash64 XXHASH64 = XXHashFactory.fastestInstance().hash64();

    public static long xxHash64(ByteBuffer key)
    {
        return XXHASH64.hash(key.asReadOnlyBuffer(), HASH_SEED);
    }

    private static final HashFunction MURMUR3_HASH32 = Hashing.murmur3_32();

    public static int murmurHash32(ByteBuffer key)
    {
        //key = key.asReadOnlyBuffer();
        //long hash = MURMUR3.hashBytes(key);
        //return (int) ((hash >> 32) & hash);
        return MURMUR3_HASH32.hashBytes(key).asInt();
    }

    public static int compare(byte[] a, byte[] b)
    {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int c = a[i] - b[i];
            if (c != 0) {
                return c;
            }
        }
        return b.length - a.length;
    }

    @Deprecated
    public static boolean equalsV2(ByteBuffer a, int oa, ByteBuffer b, int ob, int size)
    {
        return slice(a, oa, size).equals(slice(b, ob, size));
    }

    @Deprecated
    public static boolean equalsV1(ByteBuffer a, int oa, ByteBuffer b, int ob, int size)
    {
        a = a.asReadOnlyBuffer();
        a.position(oa);
        b = b.asReadOnlyBuffer();
        b.position(ob);
        while (size >= 8) {
            if (a.getLong() != b.getLong()) {
                return false;
            }
            size -= 8;
        }

        while (size >= 4) {
            if (a.getInt() != b.getInt()) {
                return false;
            }
            size -= 4;
        }
        while (size >= 2) {
            if (a.getShort() != b.getShort()) {
                return false;
            }
            size -= 2;
        }
        while (size >= 1) {
            if (a.get() != b.get()) {
                return false;
            }
            size -= 1;
        }

        return true;
    }

    //@Deprecated
    public static boolean equals(ByteBuffer a, int oa, ByteBuffer b, int ob, int size)
    {
        //boolean eq = true;
        for (int i = 0; i < size; i++) {
            if (a.get(oa + i) != b.get(ob + i)) {
                //eq = false;
                return false;
            }
        }
        //return eq;
        return true;
    }

    public static ByteBuffer asReadyOnly(ByteBuffer buffer, int offset, int size)
    {
        buffer = buffer.asReadOnlyBuffer();
        buffer.position(offset);
        buffer.limit(size + offset);
        return buffer;
    }

    public static void swap(ByteBuffer buffer, int i, int j, int size)
    {
        buffer = buffer.duplicate();
        byte[] tmp = new byte[size];
        buffer.position(i);
        buffer.get(tmp);

        buffer.position(i);
        buffer.put(slice(buffer, j, size));

        buffer.position(j);
        buffer.put(tmp);
    }

    public static int[] offsetSort(ByteBuffer buffer, int start, int end, int bucketSize, int keyOffset, BufferComparator comparator)
    {
        int[] offsets = generateOffset(start, end, bucketSize, keyOffset);
        List<Integer> ints = new AbstractList<Integer>()
        {
            @Override
            public Integer get(int index)
            {
                return offsets[index];
            }

            @Override
            public Integer set(int index, Integer element)
            {
                int old = offsets[index];
                offsets[index] = element;
                return old;
            }

            @Override
            public int size()
            {
                return offsets.length;
            }
        };
        ints.sort(new Comparator<Integer>()
        {
            @Override
            public int compare(Integer o1, Integer o2)
            {
                return comparator.compare(buffer, o1, buffer, o2);
            }
        });
        return offsets;
    }

    public static int[] timSort(ByteBuffer buffer, int start, int end, int bucketSize, int keyOffset, BufferComparator comparator)
    {
        int[] offsets = generateOffset(start, end, bucketSize, keyOffset);
        TimIntSort.sort(offsets, new IntComparator()
        {
            @Override
            public int compare(int a, int b)
            {
                return comparator.compare(buffer, a, buffer, b);
            }
        });
        return offsets;
    }

    private static int[] generateOffset(int start, int end, int bucketSize, int keyOffset)
    {
        int size = end - start;
        int count = size / bucketSize;
        int[] offsets = new int[count];
        if (offsets.length > 0) {
            offsets[0] = start + keyOffset;
        }
        for (int i = 1; i < offsets.length; i++) {
            offsets[i] = bucketSize + offsets[i - 1];
        }
        return offsets;
    }

    @Deprecated
    public static void quickSort(ByteBuffer buffer, int start, int end, int bucketSize, int keyOffset, int keySize, BufferComparator comparator)
    {
        if (end - start > bucketSize) {
            int mediumOffset = mediumSpilt(buffer, start, end, bucketSize, keyOffset, keySize, comparator);
            quickSort(buffer, start, mediumOffset, bucketSize, keyOffset, keySize, comparator);
            quickSort(buffer, mediumOffset + bucketSize, end, bucketSize, keyOffset, keySize, comparator);
        }
    }

    public static int alignPageSize(int expect, int bucketSize)
    {
        return expect / bucketSize * bucketSize;
    }

    private static int mediumSpilt(ByteBuffer buffer, int start, int end, int bucketSize, int keyOffset, int keySize, BufferComparator comparator)
    {
        int a = start;
        int b = end - bucketSize;
        while (a < b) {
            while (a < b && compare(buffer, a + keyOffset, buffer, b + keyOffset, keySize, comparator) <= 0) {
                a += bucketSize;
            }
            if (a >= b) {
                break;
            }
            swap(buffer, a, b, bucketSize);
            b -= bucketSize;
            while (a < b && compare(buffer, b + keyOffset, buffer, a + keyOffset, keySize, comparator) >= 0) {
                b -= bucketSize;
            }
            if (a >= b) {
                break;
            }
            swap(buffer, a, b, bucketSize);
            a += bucketSize;
        }
        return a;
    }

    @Deprecated
    public static ByteBuffer[] mergeSort(ByteBuffer workspace, ByteBuffer data, int bucketSize, int size, int pageSize, int keySize, BufferComparator comparator)
    {
        mergeSortPartialPhase(data, bucketSize, size, pageSize, keySize, comparator);
        return mergeSortFinalPhase(workspace, data, pageSize, bucketSize, size, comparator);
    }

    @Deprecated
    public static void mergeSortPartialPhase(ByteBuffer buffer, int bucketSize, int size, int pageSize, int keySize, BufferComparator comparator)
    {
        int offset = 0;
        while (offset < size) {
            int mSize = Math.min(pageSize, size - offset);
            BufferUtil.quickSort(buffer, offset, mSize + offset, bucketSize, 0, keySize, comparator);

            offset += mSize;
        }
    }

    @Deprecated
    public static ByteBuffer[] mergeSortFinalPhase(ByteBuffer workspace, ByteBuffer buffer, int pageSize, int bucketSize, int size, BufferComparator comparator)
    {
        while (pageSize < size) {
            workspace.clear();
            int offset = 0;
            while (offset + pageSize < size) {
                int regionMiddle = offset + pageSize;
                int regionSize = Math.min(pageSize * 2, size - offset);
                int regionEnd = offset + regionSize;
                int pLeft = offset;
                int pRight = regionMiddle;

                while (pLeft < regionMiddle && pRight < regionEnd) {
                    int c = BufferUtil.compare(buffer, pLeft, buffer, pRight, bucketSize, comparator);
                    if (c <= 0) {
                        BufferUtil.copy(buffer, pLeft, bucketSize, workspace);
                        pLeft += bucketSize;
                    }
                    else {
                        BufferUtil.copy(buffer, pRight, bucketSize, workspace);
                        pRight += bucketSize;
                    }
                }
                if (pLeft < regionMiddle) {
                    BufferUtil.copy(buffer, pLeft, regionMiddle - pLeft, workspace);
                }
                if (pRight < regionEnd) {
                    BufferUtil.copy(buffer, pRight, regionEnd - pRight, workspace);
                }

                offset = regionEnd;
            }
            if (offset < size) {
                BufferUtil.copy(buffer, offset, size - offset, workspace);
            }
            workspace.flip();

            ByteBuffer tmp = buffer;
            buffer = workspace;
            workspace = tmp;

            pageSize = pageSize << 1;
        }

        return new ByteBuffer[] {buffer, workspace};
    }

    public static int compare(ByteBuffer a, int oa, ByteBuffer b, int ob, int size, BufferComparator comparator)
    {
        return comparator.compare(a, oa, b, ob);
    }

    @Deprecated
    public static void copyV2(ByteBuffer src, int start, int size, ByteBuffer dst)
    {
        int end = start + size;
        while (start < end) {
            dst.put(src.get(start++));
        }
    }

    @Deprecated
    public static void copyV1(ByteBuffer src, int start, int size, ByteBuffer dst)
    {
        while (size >= 8) {
            dst.putLong(src.getLong(start));
            start += 8;
            size -= 8;
        }
        while (size >= 4) {
            dst.putInt(src.getInt(start));
            start += 4;
            size -= 4;
        }
        while (size >= 2) {
            dst.putShort(src.getShort(start));
            start += 2;
            size -= 2;
        }
        while (size >= 1) {
            dst.put(src.get(start));
            start += 1;
            size -= 1;
        }
    }

    //@Deprecated
    public static void copy(ByteBuffer src, int start, int size, ByteBuffer dst)
    {
        src = src.asReadOnlyBuffer();
        src.position(start);
        src.limit(start + size);
        dst.put(src);
    }

    public static ByteBuffer slice(ByteBuffer buffer, int offset, int size)
    {
        buffer = buffer.asReadOnlyBuffer();
        buffer.position(offset);
        buffer = buffer.slice();
        buffer.limit(size);
        return buffer;
    }
}
