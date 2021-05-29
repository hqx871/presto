package org.apache.cstore.column;

import org.apache.cstore.bitmap.Bitmap;
import org.apache.cstore.bitmap.RoaringBitmapAdapter;
import org.apache.cstore.dictionary.TrieHeapTree;
import org.apache.cstore.io.OutputStreamWriter;
import org.apache.cstore.io.StreamWriter;
import org.apache.cstore.io.VectorWriter;
import org.apache.cstore.io.VectorWriterFactory;
import org.apache.cstore.util.IOUtil;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

public class StringEncodedColumnWriter
        implements VectorWriter<String>
{
    private final DataOutputStream dataStream;

    private final TrieHeapTree dict;
    private final SortedMap<Integer, MutableRoaringBitmap> bitmaps;
    private final VectorWriterFactory writerFactor;
    private int rowNum;
    private final File dataFile;
    private final File finalFile;

    public StringEncodedColumnWriter(TrieHeapTree dict, VectorWriterFactory writerFactor)
    {
        this.writerFactor = writerFactor;
        this.dataFile = writerFactor.newFile(writerFactor.getName() + ".data");
        this.dataStream = IOUtil.openFileDataStream(dataFile);
        this.dict = dict;

        this.bitmaps = new TreeMap<>();
        this.bitmaps.put(0, new MutableRoaringBitmap());
        this.rowNum = 0;
        this.finalFile = writerFactor.newFile(writerFactor.getName() + ".bin");
    }

    @Override
    public int write(String value)
    {
        try {
            int id = dict.encode(value);
            dataStream.writeInt(id);
            MutableRoaringBitmap bitmap = bitmaps.computeIfAbsent(id, k -> new MutableRoaringBitmap());
            bitmap.add(rowNum++);
            return 0;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        IOUtil.close(dataStream);

        DataOutputStream output = IOUtil.openFileDataStream(finalFile);
        StreamWriter streamWriter = new OutputStreamWriter(output);
        flushTo(streamWriter);
        streamWriter.close();
        //IOUtil.close(output);
    }

    //@Override
    public int flushTo(StreamWriter output)
    {
        int[] newIds = dict.sortValue();
        int dictSize = dict.write(output, writerFactor);
        output.putInt(dictSize);
        int bitmapSize = writeBitmap(output, newIds);
        output.putInt(bitmapSize);
        int dataSize = writeData(output, dataFile, dict.maxEncodeId(), newIds);
        output.putInt(dataSize);
        return dictSize + bitmapSize + dataSize + 12;
    }

    private int writeBitmap(StreamWriter mergeStream, int[] newIds)
    {
        BinaryOffsetWriter<Bitmap> bitmapWriter = new BinaryOffsetWriter<>(
                new VectorWriterFactory(writerFactor.getDir(), writerFactor.getName() + ".bitmap"),
                BitmapColumnReader.coder);

        SortedMap<Integer, MutableRoaringBitmap> newBitmaps = new TreeMap<>();
        bitmaps.forEach((oldId, bitmap) -> {
            int newId = newIds[oldId];
            newBitmaps.put(newId, bitmap);
        });
        for (Entry<Integer, MutableRoaringBitmap> valueBitmap : newBitmaps.entrySet()) {
            MutableRoaringBitmap bitmap = valueBitmap.getValue();
            bitmapWriter.write(new RoaringBitmapAdapter(bitmap));
        }
        int size = bitmapWriter.flushTo(mergeStream);
        bitmapWriter.close();
        return size;
    }

    private int writeData(StreamWriter output, File tmpDataFile, int ceilId, int[] newIds)
    {
        IntBuffer ints = IOUtil.mapFile(tmpDataFile, MapMode.READ_ONLY).asIntBuffer();

        int size = 0;
        if (ceilId <= Byte.MAX_VALUE) {
            output.putByte(ColumnEncodingId.PLAIN_BYTE);
            ByteColumnPlainWriter byteRLEWriter = new ByteColumnPlainWriter(new VectorWriterFactory(writerFactor.getDir(), writerFactor.getName() + "-data"));
            while (ints.hasRemaining()) {
                byteRLEWriter.write((byte) newIds[ints.get()]);
            }
            size = 1 + byteRLEWriter.flushTo(output);
        }
        else if (ceilId <= Short.MAX_VALUE) {
            output.putByte(ColumnEncodingId.PLAIN_SHORT);
            ShortColumnWriter shortVectorWriter = new ShortColumnWriter(new VectorWriterFactory(writerFactor.getDir(), writerFactor.getName() + "-data"));
            while (ints.hasRemaining()) {
                shortVectorWriter.write((short) newIds[ints.get()]);
            }
            size = 1 + shortVectorWriter.flushTo(output);
        }
        else {
            output.putByte(ColumnEncodingId.PLAIN_INT);
            IntColumnWriter intVectorWriter = new IntColumnWriter(new VectorWriterFactory(writerFactor.getDir(), writerFactor.getName() + "-data"));
            while (ints.hasRemaining()) {
                intVectorWriter.write(newIds[ints.get()]);
            }
            size = 1 + intVectorWriter.flushTo(output);
        }
        return size;
    }
}
