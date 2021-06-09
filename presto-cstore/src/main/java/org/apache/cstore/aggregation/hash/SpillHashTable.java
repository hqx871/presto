package org.apache.cstore.aggregation.hash;

import org.apache.cstore.sort.BufferComparator;
import org.apache.cstore.util.BufferUtil;
import org.apache.cstore.util.ExecutorManager;
import org.apache.cstore.util.FileManager;
import org.apache.cstore.util.IOUtil;
import org.apache.cstore.util.MemoryManager;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public abstract class SpillHashTable
        extends LinkedHashTable
{
    private final FileManager fileManager;
    private final ExecutorManager executorManager;

    private final BufferComparator keyComparator;
    private final List<Future<File>> spillResults;
    protected final int outEntrySize;

    protected SpillHashTable(int keySize,
            int valueSize,
            int capacityBit,
            int maxCapacityBit,
            BufferComparator keyComparator,
            File tmpDir,
            ExecutorManager executorManager,
            MemoryManager memoryManager)
    {
        super(keySize, valueSize, capacityBit, maxCapacityBit, memoryManager);
        this.fileManager = new FileManager("partial-spill-%d.bin", tmpDir);
        this.executorManager = executorManager;

        this.keyComparator = keyComparator;
        this.spillResults = new ArrayList<>();
        this.outEntrySize = keySize + valueSize;
    }

    protected final synchronized void doSpill()
    {
        final int count = this.count;
        final ByteBuffer buffer = this.buffer;
        int size = getBucketOffset(count);
        this.buffer = ByteBuffer.allocate(buffer.limit());
        super.clear();

        Future<File> future = executorManager.getExecutor().submit(new Callable<File>()
        {
            @Override
            public File call()
                    throws Exception
            {
                File tmpFile = fileManager.createFile();
                writeToFile(tmpFile, sortMemoryTable(buffer, size, count));
                return tmpFile;
            }
        });
        spillResults.add(future);
    }

    private ByteBuffer sortMemoryTable(ByteBuffer buffer, int limit, int count)
    {
        int[] offsets = BufferUtil.timSort(buffer, magic, limit, bucketSize, bucketHeaderSize, keyComparator);
        ByteBuffer newBuf = compactTableBuffer(buffer, count, offsets);
        return newBuf.asReadOnlyBuffer();
    }

    private ByteBuffer compactTableBuffer(ByteBuffer buffer, int count, int[] offsets)
    {
        ByteBuffer newBuf = ByteBuffer.allocate(outEntrySize * count);
        for (int offset : offsets) {
            BufferUtil.copy(buffer, offset, outEntrySize, newBuf);
        }
        newBuf.flip();
        return newBuf;
    }

    private void writeToFile(File file, ByteBuffer buffer)
            throws IOException
    {
        FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE);
        channel.write(buffer);
        channel.close();
    }

    protected Iterator<ByteBuffer> rawIterator()
    {
        if (spillResults.size() > 0) {
            return inMemoryAndDiskIterator();
        }
        else {
            return inMemoryIterator();
        }
    }

    private Iterator<ByteBuffer> inMemoryIterator()
    {
        if (size() <= 0) {
            return Collections.emptyIterator();
        }
        ByteBuffer buffer = sortMemoryTable(this.buffer, getBucketOffset(count), count);
        int totalSize = size() * outEntrySize;

        return new Iterator<ByteBuffer>()
        {
            private int read;

            @Override
            public boolean hasNext()
            {
                return read < totalSize;
            }

            @Override
            public ByteBuffer next()
            {
                int tmp = read;
                read += outEntrySize;
                return BufferUtil.slice(buffer, tmp, outEntrySize);
            }
        };
    }

    private Iterator<ByteBuffer> inMemoryAndDiskIterator()
    {
        Queue<ByteBuffer> minHeap = new PriorityQueue<>(new Comparator<ByteBuffer>()
        {
            @Override
            public int compare(ByteBuffer a, ByteBuffer b)
            {
                return keyComparator.compare(a, 0, b, 0);
            }
        });
        if (size() > 0) {
            ByteBuffer buffer = sortMemoryTable(this.buffer, getBucketOffset(count), count);
            minHeap.offer(buffer);
        }

        List<File> files = new ArrayList<>();
        spillResults.forEach(future -> {
            try {
                files.add(future.get());
            }
            catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        files.forEach(file -> minHeap.offer(IOUtil.mapFile(file, MapMode.READ_ONLY)));

        return new Iterator<ByteBuffer>()
        {
            @Override
            public boolean hasNext()
            {
                return minHeap.size() > 0;
            }

            @Override
            public ByteBuffer next()
            {
                return minHeap.poll();
            }
        };
    }

    protected boolean isDistinct()
    {
        return spillResults.isEmpty();
    }
}
